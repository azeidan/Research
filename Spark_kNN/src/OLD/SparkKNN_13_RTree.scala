package org.cusp.bdi.sknn

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.SortedSet
import scala.util.Random

import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.DistanceMeasure
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.serializer.KryoSerializer
import org.cusp.bdi.gm.GeoMatch
import org.cusp.bdi.gm.geom.GMGeomBase
import org.cusp.bdi.gm.geom.GMPoint
import org.cusp.bdi.sknn.util.RDD_Store
import org.cusp.bdi.sknn.util.SparkKNN_Arguments
import org.cusp.bdi.sknn.util.SparkKNN_Local_CLArgs
import org.cusp.bdi.util.Helper
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.index.strtree.AbstractNode
import org.locationtech.jts.index.strtree.ItemBoundable
import org.locationtech.jts.index.strtree.STRtree
import org.apache.hadoop.io.compress.GzipCodec
import org.cusp.bdi.sknn.util.STRtreeOperations
import breeze.linalg.functions.euclideanDistance
import scala.collection.mutable.Set
import org.cusp.bdi.sknn.util.HilbertIndex_V2
import org.cusp.bdi.gm.geom.util.LineRasterization

case class RowWrapper(_k: Int, _pointXY: (String, String)) extends Serializable with Ordered[RowWrapper] {

    def pointXY = _pointXY
    def k = _k

    override def compare(that: RowWrapper): Int =
        this.pointXY._1.toDouble.compareTo(that.pointXY._1.toDouble)
}

case class Region(xy1: (Double, Double), xy2: (Double, Double)) extends Serializable /*with Ordered[MBR]*/ {

    var uniqueID: String = null
    var pointCount = 0L
    var assignedPart = "-1"
    var minX = xy1._1
    var minY = xy1._2
    var maxX = xy2._1
    var maxY = xy2._2

    def this() =
        this((Double.PositiveInfinity, Double.PositiveInfinity), (Double.NegativeInfinity, Double.NegativeInfinity))

    def contains(pointXY: (Double, Double)) =
        pointXY._1 >= minX && pointXY._1 <= maxX && pointXY._2 >= minY && pointXY._2 <= maxY

    def diffFromCenter(pointXY: (Double, Double)) =
        (math.abs(center._1 - pointXY._1), math.abs(center._2 - pointXY._2))

    def add(pointXY: (Double, Double), hilbertN: Long) {

        if (pointXY._1 < minX) minX = pointXY._1
        if (pointXY._2 < minY) minY = pointXY._2
        if (pointXY._1 > maxX) maxX = pointXY._1
        if (pointXY._2 > maxY) maxY = pointXY._2

        pointCount += 1
    }

    def width = maxX - minX

    def height = maxY - minY

    def center =
        (minX + (width / 2), minY + (height / 2))

    def get() =
        ((minX, minY, maxX, maxY))

    override def toString() =
        "%s\t%s\t%.8f\t%.8f\t%.8f\t%.8f\t%d".format(assignedPart, uniqueID, minX, minY, maxX, maxY, pointCount /*, setHilbert.mkString(",")*/ )
}

object SparkKNN {

    def main(args: Array[String]): Unit = {

        val startTime = System.currentTimeMillis()
        var startTime2 = startTime
        val hilbertN = math.pow(2, 31).toLong

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

        val numParts = rddDS1Plain.getNumPartitions
        val maxPartRows = (sc.runJob(rddDS1Plain, getIteratorSize _, Array(0, numParts / 2, numParts - 1)).sum / 3)

        var rddDS1_XY = rddDS1Plain
            .mapPartitions(_.map(row => (row._2._1, row._2._2)))
            //            .reduceByKey(_ + _) // eliminate duplicates
            //            .repartitionAndSortWithinPartitions(new HashPartitioner(numParts)) // group along the x-axis
            .persist()

        val kmeansK = numParts

        var kmeans = new KMeans().setK(kmeansK).setSeed(1L).setDistanceMeasure(DistanceMeasure.EUCLIDEAN)
        var kmModel = kmeans.run(rddDS1_XY.mapPartitions(_.map(row => Vectors.dense(row._1.toDouble, row._2.toDouble))))

        kmeans = null

        println(">T>Kmeans: %f".format((System.currentTimeMillis() - startTime2) / 1000.0))

        startTime2 = System.currentTimeMillis()

        val partitionerByK = new Partitioner() {

            override def numPartitions = kmeansK
            override def getPartition(key: Any): Int =
                key match {
                    case keyBase: KeyBase => keyBase.partId.toInt
                    case rowWrapper: RowWrapper => rowWrapper.k
                    case partId: Int => partId
                }
        }

        var arrRegion = rddDS1_XY.mapPartitions(_.map(row =>
            (RowWrapper(kmModel.predict(Vectors.dense(row._1.toDouble, row._2.toDouble)), row), null)))
            //            .repartitionAndSortWithinPartitions(partitionerByK) // sorts along the X-axis
            .partitionBy(partitionerByK)
            .mapPartitionsWithIndex((pIdx, iter) => {

                var rowK = -1
                val retList = ListBuffer[Region]()

                iter.foreach(row => {

                    val pointXY = (row._1.pointXY._1.toDouble, row._1.pointXY._2.toDouble)

                    if (rowK == -1)
                        rowK = row._1.k

                    if (rowK != row._1.k)
                        throw new Exception("Found more than one K on the same partition. Should not happen at this point (k==numParts). (%d and %d)".format(rowK, row._1))

                    if (retList.isEmpty || (retList.last.pointCount >= maxPartRows && !retList.last.contains(pointXY))) {

                        val region = new Region()

                        if (retList.isEmpty)
                            region.assignedPart = rowK.toString
                        else // for load balancing
                            region.assignedPart = (-rowK).toString // for load balancing

                        retList.append(region)
                    }

                    retList.last.add(pointXY, hilbertN)
                })

                retList.iterator
            })
            .collect

        rddDS1_XY.unpersist(false)
        rddDS1_XY = null

        //        arrRegion.foreach(row => println(">>%s".format(row)))

        val lstGlobalIndex: ListBuffer[ListBuffer[Region]] = ListBuffer.fill(numParts) { null }

        (0 until arrRegion.length).foreach(i => {

            val region = arrRegion(i)
            region.uniqueID = i.toString

            if (region.assignedPart.charAt(0) != '-')
                lstGlobalIndex(region.assignedPart.toInt) = ListBuffer(region)
        })

        val adjustedMaxPartRows = math.ceil(maxPartRows * 1.1).toInt // allows 10% increase in max row count when merging partitions (favors proximity)

        (0 until arrRegion.length).foreach(i => {

            val region = arrRegion(i)

            if (region.assignedPart.charAt(0) == '-') {

                // find an under utilized partition.
                val openPart = (lstGlobalIndex.size - 1 to 0 by -1).map(i => {

                    val lst = lstGlobalIndex(i)

                    val sum = if (lst.size == 1) lst(0).pointCount else lst.map(_.pointCount).sum

                    (sum, lst)
                })
                    .filter(_._1 + region.pointCount <= adjustedMaxPartRows)
                    .take(1)

                if (openPart.size == 1) {

                    region.assignedPart = openPart(0)._2(0).assignedPart
                    openPart(0)._2.append(region)
                }
                else /*if (lstPartToMBR.size == numParts)*/ {

                    region.assignedPart = lstGlobalIndex.size.toString()
                    lstGlobalIndex.append(ListBuffer(region))
                }
            }
        })

        arrRegion = null

        //        lstGlobalIndex.map(lst => println(">>\t%s".format(lst.mkString("\t"))))

        val partitionerByK2 = new Partitioner() {

            override def numPartitions = lstGlobalIndex.size
            override def getPartition(key: Any): Int =
                key match {
                    case keyBase: KeyBase => keyBase.partId.toInt
                    case partId: Int => partId
                }
        }

        val rddDS1 = rddDS1Plain
            .mapPartitionsWithIndex((pIdx, iter) => iter.map(row => {

                // Compute best partition
                val pointXY = (row._2._1.toDouble, row._2._2.toDouble)

                // val k = kmModel.predict(Vectors.dense(pointXY._1, pointXY._2))

                val bestRegion = getClosestRegion(pointXY, lstGlobalIndex, maxPartRows)(0)

                val key: KeyBase = Key0(bestRegion._2.assignedPart)
                key.regionID = bestRegion._2.uniqueID

                (key, row)
            }))
            .partitionBy(partitionerByK2)
            .mapPartitionsWithIndex((pIdx, iter) => {

                val jtsGeomFact = new GeometryFactory

                var row: (KeyBase, (String, (String, String))) = null

                val mapRegionTree = HashMap[String, STRtree]()

                while (iter.hasNext) {

                    row = iter.next

                    val gmGeom: GMGeomBase = new GMPoint(row._2._1, (row._2._2._1, row._2._2._2))

                    //                    if (gmGeom.payload.equalsIgnoreCase("Ra_801763") ||
                    //                        gmGeom.payload.equalsIgnoreCase("Ra_363965") ||
                    //                        gmGeom.payload.equalsIgnoreCase("Ra_105357"))
                    //                        println

                    val env = gmGeom.toJTS(jtsGeomFact)(0).getEnvelopeInternal

                    val sTRtree = mapRegionTree.getOrElse(row._1.regionID, {

                        val newTree = new STRtree

                        mapRegionTree.put(row._1.regionID, newTree)

                        newTree
                    })

                    sTRtree.insert(env, (gmGeom, SortSetObj(kParam)))
                }

                val ret: (KeyBase, (HashMap[String, STRtree], GMGeomBase, SortSetObj, ListBuffer[Int])) =
                    (row._1, (mapRegionTree, null, null, null))

                Iterator(ret)
            }, true)
            .partitionBy(partitionerByK2)

        val rddDS2 = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), minPartitions)
            .mapPartitionsWithIndex((pIdx, iter) => {

                val lineParser = RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj))

                val jtsGeomFact = new GeometryFactory

                iter.map(line => {

                    val row = lineParser(line)

                    val pointXY = (row._2._1.toDouble, row._2._2.toDouble)

                    //                    if (row._1.equalsIgnoreCase("Rb_999346"))
                    //                        println

                    val gmGeom: GMGeomBase = new GMPoint(row._1, (row._2._1, row._2._2))

                    val bestRegion = getClosestRegion(pointXY, lstGlobalIndex, maxPartRows)(0)

                    val key: KeyBase = Key1(bestRegion._2.assignedPart)
                    key.regionID = bestRegion._2.uniqueID

                    val ret: (KeyBase, (HashMap[String, STRtree], GMGeomBase, SortSetObj, ListBuffer[Int])) =
                        (key, (null, gmGeom, SortSetObj(kParam), null))

                    //                    println(">>%s\t%s\t%s".format(ret._1, ret._2._2.payload, ret._2._4.mkString("\t")))

                    ret
                })
            })
            .partitionBy(partitionerByK2)

        var rddResult = rddDS1.union(rddDS2)
        //        (0 until 8).foreach(i => {

        //            if (i > 0)
        //                rddResult = rddResult.repartitionAndSortWithinPartitions(partitionerByK2)

        rddResult = rddResult.mapPartitionsWithIndex((pIdx, iter) => {

            var mapRegionTree: HashMap[String, STRtree] = null
            val jtsGeomFact = new GeometryFactory
            var ds1Row: (KeyBase, (HashMap[String, STRtree], GMGeomBase, SortSetObj, ListBuffer[Int])) = null

            iter.map(row => {

                row._1 match {
                    case _: Key0 => {

                        ds1Row = row

                        mapRegionTree = row._2._1

                        if (iter.hasNext)
                            null
                        else
                            Iterator(ds1Row)
                    }
                    case _: Key1 => {

                        var ds2Row: (KeyBase, (HashMap[String, STRtree], GMGeomBase, SortSetObj, ListBuffer[Int])) = null

                        val (_, gmGeom, gmGeomSet, _) = row._2

                        //                        if (closestParts == null)
                        //                            ds2Row = row
                        //                        else {

                        if (mapRegionTree != null) {

                            val sTRtree = mapRegionTree.get(row._1.regionID).get
                            STRtreeOperations.rTreeNearestNeighbor(jtsGeomFact, gmGeom, gmGeomSet, kParam, sTRtree)
                        }

                        //                            if (closestParts.size == 0)
                        ds2Row = (row._1, (null, gmGeom, gmGeomSet, null))
                        //                            else {
                        //
                        //                                row._1.k = closestParts(0)
                        //                                closestParts.remove(0)
                        //                                ds2Row = (row._1, (null, gmGeom, gmGeomSet, closestParts))
                        //                            }
                        //                        }

                        if (iter.hasNext)
                            Iterator(ds2Row)
                        else
                            Iterator(ds1Row, ds2Row)
                    }
                }
            })
                .filter(_ != null)
                .flatMap(_.seq)
        }, true)
        //        })

        rddResult.mapPartitions(_.map(row => {

            row._2._1 match {
                case mapRegionTree: HashMap[String, STRtree] => {

                    mapRegionTree.values.map(sTRtree => {

                        val lst = ListBuffer[(GMGeomBase, SortSetObj)]()

                        getTreeItems(sTRtree.getRoot, lst)

                        lst.iterator
                    })
                        .flatMap(_.seq)
                }
                case _ => Iterator((row._2._2, row._2._3))
            }
        })
            .flatMap(_.seq)).map(x => x)
            .mapPartitions(_.map(row => "%s,%s,%s%s".format(row._1.payload, row._1.coordArr(0)._1, row._1.coordArr(0)._2, row._2.toString())))
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

    private def getClosestRegion(pointXY: (Double, Double), lstGlobalIndex: ListBuffer[ListBuffer[Region]], maxPartRows: Long) = {
        lstGlobalIndex.map(_.map(region => (Helper.squaredDist(region.center, pointXY), region)))
            .flatMap(_.seq)
            .sortBy(_._1)

    }

    private def getIteratorSize(iter: Iterator[_]) = {
        var count = 0
        var size = 0
        while (iter.hasNext) {
            count += 1
            iter.next()
        }
        count
    }
}