//package org.cusp.bdi.sknn.keygen
//
//import org.apache.hadoop.io.compress.GzipCodec
//import org.apache.spark.Partitioner
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.serializer.KryoSerializer
//import org.cusp.bdi.gm.GeoMatch
//import org.cusp.bdi.gm.geom.GMGeomBase
//import org.cusp.bdi.gm.geom.GMPoint
//import org.cusp.bdi.sknn.Key0
//import org.cusp.bdi.sknn.Key1
//import org.cusp.bdi.sknn.KeyBase
//import org.cusp.bdi.sknn.ResultRDDtoString
//import org.cusp.bdi.sknn.SortSetObj
//import org.cusp.bdi.sknn.util.RDD_Store
//import org.cusp.bdi.sknn.util.STRtreeOperations
//import org.cusp.bdi.sknn.util.SparkKNN_Arguments
//import org.cusp.bdi.util.Helper
//import org.cusp.bdi.util.sknn.SparkKNN_Local_CLArgs
//import org.locationtech.jts.geom.GeometryFactory
//import org.locationtech.jts.index.strtree.STRtree
//
//object KNN_KeyMatchGenerator {
//
//    def main(args: Array[String]): Unit = {
//
//        val startTime = System.currentTimeMillis()
//
//        val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift(SparkKNN_Arguments())
//        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift_TINY(SparkKNN_Arguments())
//        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint(SparkKNN_Arguments())
//        //        val clArgs = CLArgsParser(args, SparkKNN_Arguments())
//
//        val sparkConf = new SparkConf()
//            .setAppName(this.getClass.getName)
//            .set("spark.serializer", classOf[KryoSerializer].getName)
//            .registerKryoClasses(GeoMatch.getGeoMatchClasses())
//
//        if (clArgs.getParamValueBoolean(SparkKNN_Arguments.local))
//            sparkConf.setMaster("local[*]")
//
//        val kParam = 10 //clArgs.getParamValueInt(SparkKNN_Arguments.k)
//        val outDir = clArgs.getParamValueString(SparkKNN_Arguments.outDir)
//
//        val sc = new SparkContext(sparkConf)
//
//        // delete output dir if exists
//        Helper.delDirHDFS(sc, clArgs.getParamValueString(SparkKNN_Arguments.outDir))
//
//        val rdd1: RDD[(KeyBase, Any)] =
//            RDD_Store.getRDD(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), clArgs.getParamValueString(SparkKNN_Arguments.firstSetObjType))
//                .reduceByKey((x, y) => x) // eliminates duplicate records
//                .mapPartitionsWithIndex((pIdx, iter) => {
//
//                    val sTRtree = new STRtree
//                    val jtsGeomFact = new GeometryFactory
//
//                    while (iter.hasNext) {
//
//                        val row = iter.next
//                        val gmGeom = GMPoint(row._1, (row._2._1.toDouble, row._2._2.toDouble))
//
//                        val env = gmGeom.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//
//                        sTRtree.insert(env, (gmGeom, SortSetObj(kParam)))
//                    }
//
//                    Iterator((Key0(pIdx), sTRtree))
//                })
//
//        val rdd2: RDD[(KeyBase, Any)] = RDD_Store.getRDD(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), clArgs.getParamValueString(SparkKNN_Arguments.secondSetObjType))
//            .reduceByKey((x, y) => x) // eliminates duplicate records
//            .mapPartitionsWithIndex((pIdx, iter) =>
//                iter.map(row => {
//
//                    val gmGeom = GMPoint(row._1, (row._2._1.toDouble, row._2._2.toDouble))
//
//                    (Key1(pIdx), (gmGeom, SortSetObj(kParam)))
//                }))
//
//        val partitionerByK = new Partitioner() {
//            override def numPartitions = rdd1.getNumPartitions
//            override def getPartition(key: Any): Int = key match { case keyBase: KeyBase => keyBase.partId.toInt }
//        }
//
//        var rddResult = rdd1.partitionBy(partitionerByK)
//            .union(rdd2.partitionBy(partitionerByK))
//
//        (0 until partitionerByK.numPartitions).foreach(i => {
//
////            if (i > 0)
////                rddResult = rddResult.repartitionAndSortWithinPartitions(partitionerByK)
//
//            rddResult = rddResult
//                .mapPartitionsWithIndex((pIdx, iter) => {
//
//                    val jtsGeomFact = new GeometryFactory
//                    var sTRtree: STRtree = null
//
//                    iter.map(row =>
//                        row._1 match {
//                            case _: Key0 => {
//
//                                if (sTRtree == null)
//                                    sTRtree = row._2 match { case rt: STRtree => rt }
//
//                                if (iter.hasNext)
//                                    null
//                                else
//                                    Iterator[(KeyBase, Any)]((Key0(pIdx), sTRtree))
//                            }
//                            case _: Key1 => {
//
//                                val (gmGeom, gmGeomSet) = row._2.asInstanceOf[(GMGeomBase, SortSetObj)]
//
////                                if (sTRtree != null)
////                                    STRtreeOperations.rTreeNearestNeighbor(jtsGeomFact, gmGeom, gmGeomSet, kParam * 30, sTRtree) // *30 to lessen the boundary problem
//
//                                var retRow: (KeyBase, (GMGeomBase, SortSetObj)) =
//                                    (Key1(((row._1.partId.toInt + 1) % partitionerByK.numPartitions)), (gmGeom, gmGeomSet))
//
//                                if (iter.hasNext)
//                                    Iterator(retRow)
//                                else
//                                    Iterator((Key0(pIdx), sTRtree), retRow)
//                            }
//                        })
//                        .filter(_ != null)
//                        .flatMap(_.seq)
//                }, true)
//        })
//
//        ResultRDDtoString(rddResult)
//            .saveAsTextFile(outDir, classOf[GzipCodec])
//
//        printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
//        println(outDir)
//    }
//}
//
//
//
//
//
//
//
//
//
//
//
//
//
