//package org.cusp.bdi.sknn
//
//import scala.collection.mutable.ListBuffer
//import scala.collection.mutable.Set
//import scala.collection.mutable.SortedSet
//
//import org.apache.hadoop.io.compress.GzipCodec
//import org.apache.spark.Partitioner
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.serializer.KryoSerializer
//import org.cusp.bdi.gm.GeoMatch
//import org.cusp.bdi.gm.geom.GMGeomBase
//import org.cusp.bdi.gm.geom.GMPoint
//import org.cusp.bdi.util.Helper
//import org.cusp.bdi.util.HilbertIndex
//import org.locationtech.jts.geom.GeometryFactory
//import org.locationtech.jts.geom.Point
//import org.locationtech.jts.index.strtree.AbstractNode
//import org.locationtech.jts.index.strtree.ItemBoundable
//import org.locationtech.jts.index.strtree.ItemDistance
//import org.locationtech.jts.index.strtree.STRtree
//
//object SparkKNN_0 {
//
//    def getRDD(sc: SparkContext, fileName: String, objType: String, minPartitions: Int, hilbertSize: Int, hilbertBoxWidth: Int, hilbertBoxHeight: Int, partitionerHilbertCluster: Partitioner, partitionerBalanced: Partitioner) = {
//
//        RDD_Store.getRDD(sc, fileName, objType, minPartitions)
//            .mapPartitions(_.map(row => {
//
//                val pointCoords = (row._2._1.toInt, row._2._2.toInt)
//
//                val hIdx = HilbertIndex.computeIndex(hilbertSize, (pointCoords._1 / hilbertBoxWidth, pointCoords._2 / hilbertBoxHeight))
//
//                val gmGeom: GMGeomBase = new GMPoint(row._1, pointCoords)
//
//                (hIdx, gmGeom)
//            }))
//            .repartitionAndSortWithinPartitions(partitionerHilbertCluster)
//            .zipWithIndex()
//            .mapPartitions(_.map(row => (row._2, row._1._2)))
//            .partitionBy(partitionerBalanced)
//    }
//
//    def main(args: Array[String]): Unit = {
//
//        val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift
//        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint
//        //        val clArgs = CLArgsParser(args, SparkKNN_Arguments())
//
//        val DATASET_MARKER_0: Byte = 0
//        val DATASET_MARKER_1: Byte = 1
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
//        val hilbertSize = Math.pow(2, 19).toInt
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
//        val rddPlain1 = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)
//        val numParts = rddPlain1.getNumPartitions
//
//        val countPerPartition = sc.runJob(rddPlain1, getIteratorSize _, 0 until 1).sum.toInt // rddPlain1.mapPartitionsWithIndex((pIdx, iter) => Iterator(if (pIdx == 0) iter.map(x => 1).sum else 0)).first()
//
//        val partitionerHilbertCluster = new Partitioner() {
//            override def numPartitions = numParts
//            private def maxIdxCount = (hilbertSize / numPartitions) + 1
//            override def getPartition(key: Any): Int = key match { case k: Int => k / maxIdxCount }
//        }
//
//        val partitionerBalanced = new Partitioner() {
//            override def numPartitions = numParts
//            override def getPartition(key: Any): Int = key match { case k: Long => k.toInt / countPerPartition % numPartitions }
//        }
//
//        val partitionerShift = new Partitioner() {
//            override def numPartitions = numParts
//            override def getPartition(key: Any): Int = {
//
//                val (datasetMarker, pIdx) = key.asInstanceOf[(Byte, Int)]
//
//                if (datasetMarker == DATASET_MARKER_1)
//                    (pIdx + 1) % numPartitions
//                else
//                    pIdx
//            }
//        }
//
//        val rdd1 = getRDD(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj), minPartitions, hilbertSize, hilbertBoxWidth, hilbertBoxHeight, partitionerHilbertCluster, partitionerBalanced)
//            .mapPartitionsWithIndex((pIdx, iter) => {
//
//                val rTree = new STRtree
//                val jtsGeomFact = new GeometryFactory
//
//                iter.foreach(row => {
//                    //if (row._2.payload.startsWith("2014-09-08 02:09:22,1002731.2265481339,231911.84350943242,m15,5606,m150316,1,401738,0,95.12,85.11"))
//                    //    println()
//                    val env = row._2.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//                    env.expandBy(errorRange)
//
//                    rTree.insert(env, (row._2, SortedSet[(Double, GMGeomBase)]()))
//                })
//
//                val geom: GMGeomBase = null
//                val sSet: SortedSet[(Double, GMGeomBase)] = null
//
//                Iterator(((DATASET_MARKER_0, pIdx), (rTree, geom, sSet)))
//            }, true)
//            .partitionBy(partitionerShift)
//            .cache()
//
//        val rdd2 = getRDD(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj), minPartitions, hilbertSize, hilbertBoxWidth, hilbertBoxHeight, partitionerHilbertCluster, partitionerBalanced)
//            .mapPartitionsWithIndex((pIdx, iter) => {
//
//                iter.map(row => {
//                    if (row._2.payload.startsWith("9/8/2014 0:00,1002669.023390270,231945.876325977,bx15,5956,bx150068,0,403673,0,39.78,2721.89"))
//                        println(s">> $pIdx")
//                    val rTree: STRtree = null
//
//                    ((DATASET_MARKER_1, (numParts + pIdx - 2) % numParts), (rTree, row._2, SortedSet[(Double, GMGeomBase)]()))
//                })
//            })
//            .partitionBy(partitionerShift)
//
//        var rdd = rdd2
//
//        val maxIterations = 1 //numberOfIterations
//
//        (0 until maxIterations).foreach(i => {
//            rdd = rdd1.union(rdd.partitionBy(partitionerShift))
//                .mapPartitionsWithIndex((pIdx, iter) => {
//
//                    var rTree: STRtree = null
//                    var rTreePartIdx = -1
//                    val jtsGeomFact = new GeometryFactory
//                    var flag = true
//
//                    iter.map(row => {
//
//                        if (row._1._1 == DATASET_MARKER_0) {
//
//                            rTree = row._2._1
//                            rTreePartIdx = row._1._2
//                            null
//                        }
//                        else {
//
//                            if (rTree != null) {
//
//                                val geomSortSet: Set[(Double, GMGeomBase)] = row._2._3
//                                val jtsGeom = row._2._2.toJTS(jtsGeomFact)(0)
//                                val jtsEnv = jtsGeom.getEnvelopeInternal
//
//                                jtsEnv.expandBy(errorRange)
//
//                                import scala.collection.JavaConversions._
//
//                                rTree.nearestNeighbour(jtsEnv, jtsGeom, itemDist, kParam)
//                                    .foreach(treeMatch => {
//
//                                        val (matchGeom, matchSorSet) = treeMatch.asInstanceOf[(GMGeomBase, Set[(Double, GMGeomBase)])]
//
//                                        if (row._2._2.payload.startsWith("9/8/2014 0:00,1002669.023390270,231945.876325977,bx15,5956,bx150068,0,403673,0,39.78,2721.89"))
//                                            println(s">> $pIdx")
//
//                                        if (matchSorSet.find(x => x._2.equals(row._2._1)) == None) {
//
//                                            val dist = matchGeom.toJTS(jtsGeomFact)(0).distance(jtsGeom)
//
//                                            geomSortSet.add((dist, matchGeom))
//                                            matchSorSet.add((dist, row._2._2))
//
//                                            while (geomSortSet.size > kParam) geomSortSet.remove(geomSortSet.last)
//                                            while (matchSorSet.size > kParam) matchSorSet.remove(matchSorSet.last)
//                                        }
//                                    })
//                            }
//
//                            val ds2Row = ((DATASET_MARKER_1, pIdx), row._2)
//
//                            if (iter.hasNext)
//                                Iterator(ds2Row)
//                            else if (i + 1 < maxIterations)
//                                Iterator(ds2Row)
//                            else
//                                Array(((DATASET_MARKER_0, rTreePartIdx), (rTree, null, null)), ds2Row).iterator
//                        }
//                    })
//                        .filter(_ != null)
//                        .flatMap(_.seq)
//                })
//        })
//
//        //        val maxIterations = numberOfIterations
//        //
//        //        (0 until maxIterations).foreach(i => {
//        //
//        //            rdd = rdd
//        //                .mapPartitionsWithIndex((pIdx, iter) => {
//        //
//        //                    var rTree: STRtree = null
//        //                    var rTreePartIdx = -1
//        //                    val jtsGeomFact = new GeometryFactory
//        //                    var flag = true
//        //
//        //                    iter.map(row => {
//        //
//        //                        //                        print(">>%d>> %d $$%s$$%n".format(row._1._1, pIdx,
//        //                        //                            if (row._2 == null || row._2._2 == null)
//        //                        //                                ""
//        //                        //                            else
//        //                        //                                row._2._2.payload))
//        //
//        //                        if (row._1._1 == DATASET_MARKER_0) {
//        //
//        //                            rTree = row._2._1
//        //                            rTreePartIdx = row._1._2
//        //                            null
//        //                        }
//        //                        else {
//        //
//        //                            if (rTree != null) {
//        //
//        //                                val geomSortSet: Set[(Double, GMGeomBase)] = row._2._3
//        //                                val jtsGeom = row._2._2.toJTS(jtsGeomFact)(0)
//        //                                val jtsEnv = jtsGeom.getEnvelopeInternal
//        //
//        //                                jtsEnv.expandBy(errorRange)
//        //
//        //                                import scala.collection.JavaConversions._
//        //
//        //                                rTree.nearestNeighbour(jtsEnv, jtsGeom, itemDist, kParam)
//        //                                    .foreach(treeMatch => {
//        //
//        //                                        val (matchGeom, matchSorSet) = treeMatch.asInstanceOf[(GMGeomBase, Set[(Double, GMGeomBase)])]
//        //
//        //                                        // if (matchGeom.payload.startsWith("2014-09-08 02:09:22,1002731.2265481339,231911.84350943242,m15,5606,m150316,1,401738,0,95.12,85.11"))
//        //                                        //     println()
//        //
//        //                                        if (matchSorSet.find(x => x._2.equals(row._2._1)) == None) {
//        //
//        //                                            val dist = matchGeom.toJTS(jtsGeomFact)(0).distance(jtsGeom)
//        //
//        //                                            geomSortSet.add((dist, matchGeom))
//        //                                            matchSorSet.add((dist, row._2._2))
//        //
//        //                                            while (geomSortSet.size > kParam) geomSortSet.remove(geomSortSet.last)
//        //                                            while (matchSorSet.size > kParam) matchSorSet.remove(matchSorSet.last)
//        //                                        }
//        //                                    })
//        //                            }
//        //
//        //                            val ds2Row = ((DATASET_MARKER_1, pIdx), row._2)
//        //
//        //                            if (iter.hasNext)
//        //                                Iterator(ds2Row)
//        //                            else
//        //                                Array(((DATASET_MARKER_0, rTreePartIdx), (rTree, null, null)), ds2Row).iterator
//        //                        }
//        //                    })
//        //                        .filter(_ != null)
//        //                        .flatMap(_.seq)
//        //                }, true)
//        //
//        //            if (i + 1 < maxIterations)
//        //                rdd = rdd.repartitionAndSortWithinPartitions(partitionerShift)
//        //        })
//        //        println(rdd.toDebugString)
//        //        System.exit(0)
//        rdd.mapPartitions(_.map(row => {
//
//            if (row._1._1 == DATASET_MARKER_0) {
//
//                val lst = ListBuffer[(GMGeomBase, Set[(Double, GMGeomBase)])]()
//
//                getTreeItems(row._2._1.getRoot, lst)
//
//                lst.iterator
//            }
//            else
//                Iterator((row._2._2, row._2._3))
//        }))
//            .flatMap(_.seq)
//            .mapPartitions(_.map(row => {
//
//                val sb = StringBuilder.newBuilder
//                    .append(row._1.payload)
//                    .append(",[")
//
//                row._2.foreach(matches => {
//                    sb.append("(")
//                        .append(matches._1)
//                        .append(",")
//                        .append(matches._2.payload)
//                        .append("),")
//                })
//                sb.append("]")
//                    .toString()
//            }))
//            .saveAsTextFile(outDir, classOf[GzipCodec])
//
//        printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
//
//        println(outDir)
//    }
//
//    import scala.collection.JavaConversions._
//    def getTreeItems(node: AbstractNode, lst: ListBuffer[(GMGeomBase, Set[(Double, GMGeomBase)])]) {
//
//        node.getChildBoundables.foreach(item => {
//
//            item match {
//                case an: AbstractNode => getTreeItems(an, lst)
//                case ib: ItemBoundable => lst.append(ib.getItem().asInstanceOf[(GMGeomBase, Set[(Double, GMGeomBase)])])
//            }
//        })
//    }
//
//    def getIteratorSize(iter: Iterator[_]): Long = {
//        var count = 0
//        while (iter.hasNext) {
//            count += 1
//            iter.next()
//        }
//        count
//    }
//}