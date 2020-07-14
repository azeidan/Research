package org.cusp.bdi.sknn

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.SortedSet

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.DistanceMeasure
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.serializer.KryoSerializer
import org.cusp.bdi.gm.GeoMatch
import org.cusp.bdi.gm.geom.GMGeomBase
import org.cusp.bdi.gm.geom.GMPoint
import org.cusp.bdi.sknn.util.RDD_Store
import org.cusp.bdi.sknn.util.STRtreeOperations
import org.cusp.bdi.sknn.util.SparkKNN_Arguments
import org.cusp.bdi.sknn.util.SparkKNN_Local_CLArgs
import org.cusp.bdi.util.Helper
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.Point
import org.locationtech.jts.index.strtree.AbstractNode
import org.locationtech.jts.index.strtree.ItemBoundable
import org.locationtech.jts.index.strtree.ItemDistance
import org.locationtech.jts.index.strtree.STRtree
import org.apache.spark.mllib.clustering.KMeansModel

object SparkKNN {

    def main(args: Array[String]): Unit = {

        val startTime = System.currentTimeMillis()
        var startTime2 = startTime

        val clArgs = SparkKNN_Local_CLArgs.randomPoints_randomPoints
        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift
        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint
        //        val clArgs = CLArgsParser(args, SparkKNN_Arguments())

        val sparkConf = new SparkConf()
            .setAppName(this.getClass.getName)
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .registerKryoClasses(GeoMatch.getGeoMatchClasses())
            .registerKryoClasses(Array(classOf[KeyBase]))

        if (clArgs.getParamValueBoolean(SparkKNN_Arguments.local))
            sparkConf.setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val kParam = clArgs.getParamValueInt(SparkKNN_Arguments.k)
        //        val numIter = clArgs.getParamValueInt(SparkKNN_Arguments.numIter)
        val minPartitions = clArgs.getParamValueInt(SparkKNN_Arguments.minPartitions)
        val outDir = clArgs.getParamValueString(SparkKNN_Arguments.outDir)

        // delete output dir if exists
        Helper.delDirHDFS(sc, clArgs.getParamValueString(SparkKNN_Arguments.outDir))

        val rddDS1Plain = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)
            .mapPartitions(_.map(RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj))))
        //            .reduceByKey((x, y) => x) // get rid of duplicates
        //            .persist()

        val rddDS2Plain = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), minPartitions)
        val numParts = rddDS2Plain.getNumPartitions
        val rowsPerPartition = (sc.runJob(rddDS2Plain, getIteratorSize _, Array(0, numParts / 2, numParts - 1)).sum / 3)

        var rddDS1_XYandCount = rddDS1Plain
            .mapPartitions(_.map(row => (((row._2._1.toDouble /*% boxWidth*/ ), (row._2._2.toDouble /*% boxWidth*/ )), 1L)))
            .reduceByKey(_ + _) // grid cells and their counts ((x, y), count)
            .persist()

        //        rddDS1_XYandCount.foreach(x => println(">>%.8f\t%.8f\t%d".format(x._1._1, x._1._2, x._2)))

        val kmeansK = numParts * 10

        var kmeans = new KMeans().setK(kmeansK).setSeed(1L).setDistanceMeasure(DistanceMeasure.EUCLIDEAN)
        var kmModel = kmeans.run(rddDS1_XYandCount.mapPartitions(_.map(row => Vectors.dense(row._1._1, row._1._2))))

        //        val kmModelCenters = new Array[(Int, (Double, Double))](kmModel.clusterCenters.length)

        val arrKMModelCenters = (0 until kmModel.clusterCenters.length)
            .map(i => kmModel.clusterCenters(i))
            .map(vect => (vect(0), vect(1)))
            .toArray

        kmeans = null
        kmModel = null

        //        (0 until arrKMModelCenters.length).foreach(i => println("1>>\t%d\t%.8f\t%.8f".format(i, arrKMModelCenters(i)._1, arrKMModelCenters(i)._2)))

        println(">T>Kmeans: %f".format((System.currentTimeMillis() - startTime2) / 1000.0))

        startTime2 = System.currentTimeMillis()

        var mapKandCounts = rddDS1_XYandCount.mapPartitions(_.map(row => (closestCenter(row._1, arrKMModelCenters), row._2)))
            .reduceByKey(_ + _)
            .collect()
            .toMap

        //        mapKandCounts.foreach(x => println(">>\t%d\t%d".format(x._1, x._2)))

        println(">T>mapKandCounts: %f".format((System.currentTimeMillis() - startTime2) / 1000.0))

        rddDS1_XYandCount.unpersist(false)
        rddDS1_XYandCount = null

        startTime2 = System.currentTimeMillis()

        val mapCenterInfo: HashMap[Int, Array[Int]] = HashMap[Int, Array[Int]]() ++ sc.parallelize(arrKMModelCenters)
            .mapPartitions(_.map(coord1 => {

                val arrClosestK = arrKMModelCenters
                    .map(coord2 => if (coord1.equals(coord2))
                        null
                    else
                        (Helper.squaredDist(coord1, coord2), closestCenter(coord2, arrKMModelCenters)))
                    .filter(_ != null)
                    .to[SortedSet]
                    .toArray
                    .map(_._2)

                (closestCenter(coord1, arrKMModelCenters), arrClosestK)
            }))
            .collect

        println(">T>mapCenterInfo: %f".format((System.currentTimeMillis() - startTime2) / 1000.0))

        //        mapCenterInfo.toArray.sortBy(_._1).foreach(x => println("2>>\t%d\t%s".format(x._1, x._2.mkString("\t"))))

        startTime2 = System.currentTimeMillis()

        var startK = mapCenterInfo.map(_._1).min

        val mapGIndexRound1 = PartitionScheme(startK, rowsPerPartition, mapCenterInfo, mapKandCounts, null)

        val arrCurrK_Nearest = mapCenterInfo.get(startK).get
        val startK_part = mapGIndexRound1.get(startK).get

        startK = (0 until arrCurrK_Nearest.length)
            .map(i => if (mapGIndexRound1.get(arrCurrK_Nearest(i)).get == startK_part)
                -1
            else
                arrCurrK_Nearest(i))
            .find(_ != -1)
            .get

        val mapGIndexRound2 = PartitionScheme(startK, rowsPerPartition, mapCenterInfo, mapKandCounts, mapGIndexRound1)

        println(">T>mapGlobalIndex: %f".format((System.currentTimeMillis() - startTime2) / 1000.0))

        mapGIndexRound2.foreach(x => println(">>\t%d\t%d".format(x._1, x._2)))

        mapKandCounts = null
        val partCounter = mapGIndexRound1.values.toSet.size

        val partitionerByK = new Partitioner() {

            override def numPartitions = partCounter
            override def getPartition(key: Any): Int =
                key match {
                    case keyBase: KeyBase => mapGIndexRound1.get(keyBase.k).get
                    case _ => key.asInstanceOf[(Int, Double)]._1
                }
        }

        val rddDS1 = rddDS1Plain
            .mapPartitionsWithIndex((pIdx, iter) => iter.map(row => genKeyGeom(row, true, arrKMModelCenters, mapGIndexRound1)))
            .partitionBy(partitionerByK)

        val rddDS2 = rddDS2Plain
            .mapPartitionsWithIndex((pIdx, iter) => {

                val lineParser = RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj))

                iter.map(row => genKeyGeom(lineParser(row), false, arrKMModelCenters, mapGIndexRound1))
            })
            .partitionBy(partitionerByK)

        val itemDist = new ItemDistance() with Serializable {
            override def distance(item1: ItemBoundable, item2: ItemBoundable) = {

                val jtsGeomFact = new GeometryFactory
                item1.getItem.asInstanceOf[(GMGeomBase, SortSetObj)]._1.toJTS(jtsGeomFact)(0).distance(
                    item2.getItem.asInstanceOf[Point])
            }
        }

        var rdd = rddDS1.union(rddDS2)
            .mapPartitionsWithIndex((pIdx, iter) => {

                val sTRtree = new STRtree
                val jtsGeomFact = new GeometryFactory
                var newKey1: KeyBase = null
                var sTRtreeKey: KeyBase = null

                iter.map(row => {

                    row._1 match {
                        case _: Key0 => {

                            val env = row._2.toJTS(jtsGeomFact)(0).getEnvelopeInternal

                            sTRtree.insert(env, (row._2, SortSetObj(kParam)))

                            if (sTRtreeKey == null)
                                sTRtreeKey = row._1

                            if (iter.hasNext)
                                null
                            else
                                Iterator((row._1, sTRtree))
                        }
                        case _: Key1 => {

                            //                            if (row._2.payload.equalsIgnoreCase("B200463") ||
                            //                                row._2.payload.equalsIgnoreCase("B200321") ||
                            //                                row._2.payload.equalsIgnoreCase("B200066"))
                            //                                println()

                            val (gmGeom, gmGeomSet) = (row._2, SortSetObj(kParam))

                            //                            if (gmGeom.payload.equalsIgnoreCase("Rb_848234"))
                            //                                println

                            if (sTRtree != null)
                                STRtreeOperations.rTreeNearestNeighbor(jtsGeomFact, gmGeom, gmGeomSet, kParam * 5, sTRtree) // *5 for boundery objects

                            val newKey1 = Key1(getNextPart(row._1.k, mapGIndexRound1, mapCenterInfo))
                            val ds2Row: (KeyBase, Any) = (newKey1, (gmGeom, gmGeomSet))

                            if (iter.hasNext)
                                Iterator(ds2Row)
                            else
                                Iterator(ds2Row, (sTRtreeKey, sTRtree))
                        }
                    }
                })
                    .filter(_ != null)
                    .flatMap(_.seq)
            }, true)

        // 2nd round
        val rddResult = rdd.repartitionAndSortWithinPartitions(partitionerByK)
            .mapPartitions(iter => {

                var sTRtree: STRtree = null
                var rTreeKey: KeyBase = null
                val jtsGeomFact = new GeometryFactory

                iter.map(row => {

                    row._1 match {
                        case _: Key0 => {

                            sTRtree = row._2 match { case rt: STRtree => rt }
                            rTreeKey = row._1

                            if (iter.hasNext)
                                null
                            else
                                Iterator[Any](sTRtree)
                        }
                        case _: Key1 => {

                            val (gmGeom, gmGeomSet) = row._2.asInstanceOf[(GMGeomBase, SortSetObj)]

                            if (sTRtree != null)
                                STRtreeOperations.rTreeNearestNeighbor(jtsGeomFact, gmGeom, gmGeomSet, kParam, sTRtree)

                            val ds2Row = (gmGeom, gmGeomSet)

                            if (iter.hasNext)
                                Iterator(ds2Row)
                            else
                                Iterator[Any](sTRtree, ds2Row)
                        }
                    }
                })

                    .filter(_ != null)
                    .flatMap(_.seq)
            }, true)

        rddResult.mapPartitions(_.map(row => {

            row match {
                case sTRtree: STRtree => {

                    val lst = ListBuffer[(GMGeomBase, SortSetObj)]()

                    getTreeItems(sTRtree.getRoot, lst)

                    lst.iterator
                }
                case _ => Iterator(row.asInstanceOf[(GMGeomBase, SortSetObj)])
            }
        })
            .flatMap(_.seq))
            .mapPartitions(_.map(row => "%s,%.8f,%.8f%s".format(row._1.payload, row._1.coordArr(0)._1, row._1.coordArr(0)._2, row._2.toString())))
            .saveAsTextFile(outDir, classOf[GzipCodec])

        printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)

        println(outDir)
    }

    import scala.collection.JavaConversions._
    private def getTreeItems(node: AbstractNode, lst: ListBuffer[(GMGeomBase, SortSetObj)]) {

        node.getChildBoundables.foreach(item => {

            item match {
                case an: AbstractNode => getTreeItems(an, lst)
                case ib: ItemBoundable => lst.append(ib.getItem().asInstanceOf[(GMGeomBase, SortSetObj)])
            }
        })
    }

    //    private def euclideanDist(vect1: Vector, vect2: Vector) = {
    //
    //        val arr1 = vect1.toArray
    //        val arr2 = vect2.toArray
    //
    //        Helper.euclideanDist((arr1(0), arr1(1)), (arr2(0), arr2(1)))
    //    }

    private def getIteratorSize(iter: Iterator[_]) = {
        var count = 0
        var size = 0
        while (iter.hasNext) {
            count += 1
            iter.next()
        }
        count
    }

    //    def computeMBR(sc: SparkContext, fileName: String, datasetObj: String, minPartitions: Int) = {
    //        RDD_Store.getRDDPlain(sc, fileName, minPartitions)
    //            .mapPartitionsWithIndex((pIdx, iter) => {
    //
    //                val lineParser = RDD_Store.getLineParser(datasetObj)
    //
    //                var (minX: Long, minY: Long, maxX: Long, maxY: Long) =
    //                    iter.map(lineParser)
    //                        .map(row => (row._2._1.toDouble, row._2._2.toDouble))
    //                        .fold((Long.MaxValue, Long.MaxValue, Long.MinValue, Long.MinValue))((x, y) => {
    //
    //                            val (a, b, c, d) = x.asInstanceOf[(Long, Long, Long, Long)]
    //                            val t = y.asInstanceOf[(Long, Long)]
    //
    //                            (math.min(a, t._1), math.min(b, t._2), math.max(c, t._1), math.max(d, t._2))
    //                        })
    //                Iterator((minX, minY, maxX, maxY))
    //            })
    //            .fold((Long.MaxValue, Long.MaxValue, Long.MinValue, Long.MinValue))((x, y) => {
    //
    //                val (a, b, c, d) = x.asInstanceOf[(Long, Long, Long, Long)]
    //                val t = y.asInstanceOf[(Long, Long, Long, Long)]
    //
    //                (math.min(a, t._1), math.min(b, t._2), math.max(c, t._3), math.max(d, t._4))
    //            })
    //    }

    def getNextPart(currK: Int, mapGlobalIndex: HashMap[Int, Int], mapCenterInfo: Map[Int, Array[Int]]) = {

        val currPart = mapGlobalIndex.get(currK).get
        val lstNearest = mapCenterInfo.get(currK).get

        var i = 0
        while (i < lstNearest.size && mapGlobalIndex.get(lstNearest(i)).get == currPart) i += 1
        if (i == lstNearest.size)
            currPart + 1
        else
            lstNearest(i)
    }

    private def genKeyGeom(row: (String, (String, String)), isSet1: Boolean, arrKMModelCenters: Array[(Double, Double)], mapGlobalIndex: HashMap[Int, Int]) = {

        //        if (row._1.equalsIgnoreCase("Rb_848234") ||
        //            row._1.equalsIgnoreCase("Ra_386069"))
        //            println()

        val pointXY = (row._2._1.toDouble, row._2._2.toDouble)

        val xy = (pointXY._1 /*% boxWidth*/ , pointXY._2 /*% boxWidth*/ )

        //        val k = kmModel.predict(Vectors.dense(xy._1, xy._2))
        val k = closestCenter(xy, arrKMModelCenters)

        val gmGeom: GMGeomBase = new GMPoint(row._1, (pointXY._1, pointXY._2))

        val keyBase: KeyBase = if (isSet1) Key0(k) else Key1(k)

        (keyBase, gmGeom)
    }

    def closestCenter(pointCoords: (Double, Double), arrKMModelCenters: Array[(Double, Double)]) =
        (0 until arrKMModelCenters.length).map(i => (i, Helper.squaredDist(pointCoords, arrKMModelCenters(i))))
            .minBy(_._2)
            ._1

}