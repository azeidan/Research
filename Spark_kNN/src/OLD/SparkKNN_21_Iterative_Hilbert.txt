//package org.cusp.bdi.sknn
//
//import scala.collection.mutable.ListBuffer
//import scala.collection.mutable.Set
//import scala.collection.mutable.SortedSet
//
//import org.apache.hadoop.io.compress.GzipCodec
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.serializer.KryoSerializer
//import org.cusp.bdi.gm.GeoMatch
//import org.cusp.bdi.gm.geom.GMGeomBase
//import org.cusp.bdi.gm.geom.GMPoint
//import org.cusp.bdi.util.CLArgsParser
//import org.cusp.bdi.util.Helper
//import org.cusp.bdi.util.HilbertIndex
//import org.cusp.bdi.util.InputFileParsers
//import org.locationtech.jts.geom.GeometryFactory
//import org.locationtech.jts.index.strtree.STRtree
//import org.locationtech.jts.index.strtree.ItemDistance
//import org.locationtech.jts.index.strtree.ItemBoundable
//import org.locationtech.jts.geom.Point
//import org.apache.spark.Partitioner
//
//object SparkKNN_Iterative_Hilbert {
//
//    def getRDDPlain(sc: SparkContext, fileName: String, minPartitions: Int) =
//        if (minPartitions > 0) sc.textFile(fileName, minPartitions) else sc.textFile(fileName)
//
//    def getRDD(sc: SparkContext, fileName: String, objType: String, minPartitions: Int, datasetMarker: Byte) = {
//
//        val lineParser = objType match {
//            case s if s matches "(?i)TPEP_Point" => InputFileParsers.tpepPoints
//            case s if s matches "(?i)Taxi_Point" => InputFileParsers.taxiPoints
//            case s if s matches "(?i)Bus_Point" => InputFileParsers.busPoints
//            case s if s matches "(?i)Bus_Point_shifted" => InputFileParsers.busPoints
//        }
//
//        getRDDPlain(sc, fileName, minPartitions)
//            .mapPartitions(_.map(lineParser))
//            .filter(_ != null)
//            .mapPartitions(_.map(row => {
//
//                val pointCoords = (row._2._1.toInt, row._2._2.toInt)
//
//                val gmGeom: GMGeomBase = new GMPoint(row._1, pointCoords)
//
//                (datasetMarker, (gmGeom, SortedSet[(Double, GMGeomBase)]()))
//            }))
//    }
//
//    def main(args: Array[String]): Unit = {
//
//        val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift
//        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint
//        //        val clArgs = CLArgsParser(args, SparkKNN_Arguments())
//
//        val sparkConf = new SparkConf()
//            .setAppName("kNN_Test")
//            .set("spark.serializer", classOf[KryoSerializer].getName)
//            .registerKryoClasses(GeoMatch.getGeoMatchClasses())
//
//        if (clArgs.getParamValueBoolean(SparkKNN_Arguments.local))
//            sparkConf.setMaster("local[*]")
//
//        val sc = new SparkContext(sparkConf)
//
//        val kParam = clArgs.getParamValueInt(SparkKNN_Arguments.k)
//        val errorRange = clArgs.getParamValueDouble(SparkKNN_Arguments.errorRange)
//        val numberOfIterations = clArgs.getParamValueInt(SparkKNN_Arguments.numIter)
//        val minPartitions = clArgs.getParamValueInt(SparkKNN_Arguments.minPartitions)
//        val outDir = clArgs.getParamValueString(SparkKNN_Arguments.outDir)
//
//        // delete output dir if exists
//        Helper.delDirHDFS(sc, clArgs.getParamValueString(SparkKNN_Arguments.outDir))
//
//        val hilbertSize = Math.pow(2, 14).toInt
//        val hilbertBoxWidth = Int.MaxValue / hilbertSize
//        val hilbertBoxHeight = Int.MaxValue / hilbertSize
//
//        val itemDist = new ItemDistance() with Serializable {
//            override def distance(item1: ItemBoundable, item2: ItemBoundable) = {
//
//                val jtsGeomFact = new GeometryFactory
//                item1.getItem.asInstanceOf[(GMGeomBase, Set[(Double, GMGeomBase)])]._1.toJTS(jtsGeomFact)(0).distance(
//                    item2.getItem.asInstanceOf[Point])
//            }
//        }
//
//        val startTime = System.currentTimeMillis()
//
//        val rddPlain1 = getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)
//        val rddPlain2 = getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), minPartitions)
//
//        val numParts = if (rddPlain1.getNumPartitions > rddPlain2.getNumPartitions) rddPlain1.getNumPartitions else rddPlain2.getNumPartitions
//
//        val count1 = rddPlain1.mapPartitionsWithIndex((pIdx, iter) => Iterator(if (pIdx == 0) iter.map(x => 1).sum else 0)).first()
//        val count2 = rddPlain2.mapPartitionsWithIndex((pIdx, iter) => Iterator(if (pIdx == 0) iter.map(x => 1).sum else 0)).first()
//        val countPerPartition = count1 + count2
//
//        val customPartitioner = new Partitioner() {
//            override def numPartitions = numParts
//            private def maxIdxCount = (hilbertSize / numPartitions) + 1
//            override def getPartition(key: Any): Int = key.asInstanceOf[(Int, Byte)]._1 / maxIdxCount
//        }
//
//        val customPartitioner2 = new Partitioner() {
//            override def numPartitions = numParts
//            override def getPartition(key: Any): Int = {
//
//                val (datasetMarker, rowId) = key.asInstanceOf[(Byte, Long)]
//
//                (rowId / countPerPartition).toInt % numPartitions
//            }
//        }
//
//        //        val customPartitioner = new CustomPartitioner(hilbertSize, numPartitions)
//        //        val customPartitioner2 = new CustomPartitioner2(countPerPartition, numPartitions)
//
//        val rddGeom1 = getRDD(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj), minPartitions, 0)
//        val rddGeom2 = getRDD(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj), minPartitions, 1)
//
//        var rdd = rddGeom1.union(rddGeom2)
//
//        (0 until numberOfIterations).foreach(i => {
//
//            rdd = rdd.mapPartitions(_.map(row => {
//
//                val gridXY = i match {
//                    case 0 => (row._2._1.coordArr(0)._1 / hilbertBoxWidth, row._2._1.coordArr(0)._2 / hilbertBoxHeight)
//                    case 1 => (row._2._1.coordArr(0)._2 / hilbertBoxHeight, row._2._1.coordArr(0)._1 / hilbertBoxWidth)
//                    case 2 => (row._2._1.coordArr(0)._1 / hilbertBoxWidth, row._2._1.coordArr(0)._1 / hilbertBoxWidth)
//                    case 3 => (row._2._1.coordArr(0)._2 / hilbertBoxHeight, row._2._1.coordArr(0)._2 / hilbertBoxHeight)
//                    case _ => throw new IllegalArgumentException("Algorithm not setup for iterations over %d".format(i - 1));
//                }
//
//                val hIdx = HilbertIndex.computeIndex(hilbertSize, gridXY)
//
//                ((hIdx, row._1), row._2)
//            }), true)
//                .partitionBy(customPartitioner) // partition by Hilbert idx (near by idxs stay close)
//                .mapPartitions(_.map(row => (row._1._2, row._2)), true) // remove hIdx
//                .zipWithIndex() // number sequentially
//                .mapPartitions(_.map(row => ((row._1._1, row._2), row._1._2)), true) // reorder tuple => ((marker, ID), (geom,sortSet))
//                .repartitionAndSortWithinPartitions(customPartitioner2) // eq-sized partitioning
//                .mapPartitions(iter => {
//
//                    val jtsGeomFact = new GeometryFactory
//
//                    var rTree: STRtree = null
//
//                    val lstFixed = ListBuffer[(Byte, (GMGeomBase, SortedSet[(Double, GMGeomBase)]))]()
//
//                    iter.map(row => {
//
//                        if (row._1._1 == 0)
//                            lstFixed.append((row._1._1, row._2))
//                        else {
//
//                            if (rTree == null) {
//
//                                rTree = new STRtree
//
//                                lstFixed.foreach(x => {
//
//                                    val env = x._2._1.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//                                    env.expandBy(errorRange)
//
//                                    rTree.insert(env, x._2)
//                                })
//
//                                rTree.build()
//                            }
//
//                            if (rTree.size() > 0) {
//
//                                val sSet: Set[(Double, GMGeomBase)] = row._2._2
//                                val jtsGeom = row._2._1.toJTS(jtsGeomFact)(0)
//                                val jtsEnv = jtsGeom.getEnvelopeInternal
//
//                                jtsEnv.expandBy(errorRange)
//
//                                import scala.collection.JavaConversions._
//                                rTree.nearestNeighbour(jtsEnv, jtsGeom, itemDist, kParam)
//                                    .foreach(matchGeom => {
//
//                                        val (matchGMGeom, matchSortedSet) = matchGeom.asInstanceOf[(GMGeomBase, Set[(Double, GMGeomBase)])]
//
//                                        val dist = matchGMGeom.toJTS(jtsGeomFact)(0).distance(jtsGeom)
//
//                                        row._2._2.add((dist, matchGMGeom))
//                                        matchSortedSet.add((dist, row._2._1))
//
//                                        while (row._2._2.size > kParam) row._2._2.remove(row._2._2.last)
//                                        while (matchSortedSet.size > kParam) matchSortedSet.remove(matchSortedSet.last)
//                                    })
//                            }
//                        }
//                        (row._1._1, (row._2))
//                    })
//                        //                        .filter(_ != null)
//                        .++(lstFixed.iterator)
//                }, true)
//        })
//
//        rdd.mapPartitions(_.map(row => {
//
//            val sb = StringBuilder.newBuilder
//                .append(row._2._1.payload)
//                .append(",[")
//
//            row._2._2.foreach(matches => {
//                sb.append("(")
//                    .append(matches._1)
//                    .append(",")
//                    .append(matches._2.payload)
//                    .append("),")
//            })
//            sb.append("]")
//                .toString()
//        }))
//            .saveAsTextFile(outDir, classOf[GzipCodec])
//
//        printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
//
//        println(outDir)
//    }
//}