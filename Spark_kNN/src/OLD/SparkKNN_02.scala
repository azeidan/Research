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
//import org.locationtech.jts.geom.Coordinate
//
//object SparkKNN_2 {
//
//    private val DATASET_MARKER_0: Byte = 0
//    private val DATASET_MARKER_1: Byte = 1
//
//    def getRDD(sc: SparkContext, /*dataSetMarker: Byte,*/ fileName: String, objType: String, minPartitions: Int, partitionerDistance: Partitioner) = {
//
//        /*var rdd =*/ RDD_Store.getRDD(sc, fileName, objType, minPartitions)
//            .mapPartitionsWithIndex((pIdx, iter) => {
//
//                val jtsGeomFact = new GeometryFactory
//                val pointOrig = jtsGeomFact.createPoint(new Coordinate(0, 0))
//
//                iter.map(row => {
//
//                    val pointCoords = (row._2._1.toInt, row._2._2.toInt)
//
//                    val gmGeom: GMGeomBase = new GMPoint(row._1, pointCoords)
//
//                    val dist = gmGeom.toJTS(jtsGeomFact)(0).distance(pointOrig)
//
//                    (dist, gmGeom)
//                })
//            })
//            .partitionBy(partitionerDistance)
//
//        //        rdd = dataSetMarker match {
//        //            case DATASET_MARKER_0 => rdd.repartitionAndSortWithinPartitions(partitionerDistance)
//        //            case DATASET_MARKER_1 => rdd.partitionBy(partitionerDistance)
//        //        }
//        //        rdd .mapPartitions(_.map(_._2)) // remove distance
//        //            .zipWithIndex() // enumerate records
//        //            .mapPartitions(_.map(row => (row._2, row._1))) // make row ID the key
//        //            .partitionBy(partitionerEquaCount) // equal-count partitioning
//
//        //        rdd1 = dataSetMarker match {
//        //            case DATASET_MARKER_0 => rdd1.mapPartitionsWithIndex(f, preservesPartitioning)
//        //            case DATASET_MARKER_1 => rdd.partitionBy(partitionerDistance)
//        //        }
//        //
//        //        rdd.mapPartitions(_.map(row => (dataSetMarker, row._2))) // remove the key
//        //        RDD_Store.getRDD(sc, fileName, objType, minPartitions)
//        //            .mapPartitions(_.map(row => {
//        //
//        //                val pointCoords = (row._2._1.toInt, row._2._2.toInt)
//        //
//        //                val hIdx = HilbertIndex.computeIndex(hilbertSize, (pointCoords._1 / hilbertBoxWidth, pointCoords._2 / hilbertBoxHeight))
//        //
//        //                val gmGeom: GMGeomBase = new GMPoint(row._1, pointCoords)
//        //
//        //                (hIdx, gmGeom)
//        //            }))
//        //            .repartitionAndSortWithinPartitions(partitionerHilbertCluster)
//        //            .zipWithIndex()
//        //            .mapPartitions(_.map(row => (row._2, row._1._2)))
//        //            .partitionBy(partitionerBalanced)
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
//        //        val hilbertSize = Math.pow(2, 19).toInt
//        //        val hilbertBoxWidth = Int.MaxValue / hilbertSize
//        //        val hilbertBoxHeight = Int.MaxValue / hilbertSize
//
//        val startTime = System.currentTimeMillis()
//
//        val rddPlain1 = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)
//        val numParts = rddPlain1.getNumPartitions
//        val radiusSegment = Int.MaxValue / numParts + 1
//        val countPerPartition = sc.runJob(rddPlain1, getIteratorSize _, 0 until 1).sum.toInt
//
//        val partitionerDistance = new Partitioner() {
//            override def numPartitions = numParts
//            override def getPartition(key: Any): Int = key match { case k: Double => k.toInt / radiusSegment }
//        }
//
//        val partitionerEquaCount = new Partitioner() {
//            override def numPartitions = numParts
//            override def getPartition(key: Any): Int = key match { case k: Long => k.toInt / countPerPartition % numPartitions }
//        }
//
//        val partitionerInPlace = new Partitioner() {
//
//            override def numPartitions = numParts
//            override def getPartition(key: Any): Int =
//                key.asInstanceOf[(Byte, Int)]._2
//        }
//
//        //        val partitionerShift = new Partitioner() {
//        //            override def numPartitions = numParts
//        //            override def getPartition(key: Any): Int = {
//        //
//        //                val (datasetMarker, pIdx) = key.asInstanceOf[(Byte, Int)]
//        //
//        //                if (datasetMarker == DATASET_MARKER_1)
//        //                    (pIdx + 1) % numPartitions
//        //                else
//        //                    pIdx
//        //            }
//        //        }
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
//        //        def shiftPartitionDown(pIdx: Int) = (numParts + pIdx - 1) % numParts
//        //        def shiftPartitionUp(pIdx: Int) = (pIdx + 1) % numParts
//
//        def shiftPartition(pIdx: Int, shiftBy: Int) =
//            if (pIdx == 0)
//                1
//            else
//                (numParts + pIdx + shiftBy) % numParts
//
//        val rdd1 = getRDD(sc, /*DATASET_MARKER_0,*/ clArgs.getParamValueString(SparkKNN_Arguments.firstSet), clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj), minPartitions, partitionerDistance)
//            .mapPartitions(_.map(_._2)) // remove distance
//            .zipWithIndex() // enumerate records
//            .mapPartitions(_.map(row => (row._2, row._1))) // make row ID the key
//            .partitionBy(partitionerEquaCount) // equal-count partitioning
//            .mapPartitionsWithIndex((pIdx, iter) => {
//
//                val rTree = new STRtree
//                val jtsGeomFact = new GeometryFactory
//
//                iter.foreach(row => {
//
//                    val env = row._2.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//                    env.expandBy(errorRange)
//
//                    rTree.insert(env, (row._2, SortedSet[(Double, GMGeomBase)]()))
//                })
//
//                val geom: GMGeomBase = null
//                val sSet: SortedSet[(Double, GMGeomBase)] = null
//
//                Iterator(((DATASET_MARKER_0, pIdx), (rTree, 0.0, geom, sSet)))
//            }, true)
//            //            .partitionBy(partitionerShift)
//            .partitionBy(partitionerInPlace)
//
//        val rdd2 = getRDD(sc, /*DATASET_MARKER_1,*/ clArgs.getParamValueString(SparkKNN_Arguments.secondSet), clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj), minPartitions, partitionerDistance)
//            .zipWithIndex() // enumerate records
//            .mapPartitions(_.map(row => (row._2, row._1))) // make row ID the key
//            .partitionBy(partitionerEquaCount)
//            .mapPartitionsWithIndex((pIdx, iter) => {
//
//                iter.map(row => {
//
//                    val rTree: STRtree = null
//
//                    ((DATASET_MARKER_1, pIdx), (rTree, row._2._1, row._2._2, SortedSet[(Double, GMGeomBase)]()))
//                })
//            }, true)
//            .partitionBy(partitionerInPlace)
//        //            .partitionBy(partitionerShift)
//
//        var rdd = rdd1.union(rdd2)
//
//        val maxIterations = 2 //numberOfIterations
//
//        (0 until maxIterations).foreach(i => {
//            rdd = rdd
//                .mapPartitionsWithIndex((pIdx, iter) => {
//
//                    var rTree: STRtree = null
//                    var rTreePartIdx = -1
//                    val jtsGeomFact = new GeometryFactory
//                    var flag = true
//                    var shiftCountUp = 0
//                    var shiftCountDown = 0
//
//                    iter.map(row => {
//
//                        if (row._1._1 == DATASET_MARKER_0) {
//
//                            rTree = row._2._1
//                            rTreePartIdx = row._1._2
//
//                            if (iter.hasNext)
//                                null
//                            else
//                                Iterator(row)
//                        }
//                        else {
//
//                            if (rTree != null) {
//
//                                val geomSortSet: Set[(Double, GMGeomBase)] = row._2._4
//                                val distFromOrig = row._2._2
//                                val jtsGeom = row._2._3.toJTS(jtsGeomFact)(0)
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
//                                        // if (matchGeom.payload.startsWith("2014-09-08 02:09:22,1002731.2265481339,231911.84350943242,m15,5606,m150316,1,401738,0,95.12,85.11"))
//                                        //     println()
//
//                                        if (matchSorSet.find(x => x._2.equals(row._2._1)) == None) {
//
//                                            val dist = matchGeom.toJTS(jtsGeomFact)(0).distance(jtsGeom)
//
//                                            geomSortSet.add((dist, matchGeom))
//                                            matchSorSet.add((dist, row._2._3))
//
//                                            while (geomSortSet.size > kParam) geomSortSet.remove(geomSortSet.last)
//                                            while (matchSorSet.size > kParam) matchSorSet.remove(matchSorSet.last)
//                                        }
//                                    })
//                            }
//
//                            var shiftDir = if (row._2._2 - ((pIdx + 1) * radiusSegment) > radiusSegment / 2) 1 else -1
//
//                            if (shiftDir == -1) {
//                                if (shiftCountDown > countPerPartition / 2)
//                                    shiftDir = 1
//                            }
//                            else if (shiftCountUp > countPerPartition / 2)
//                                shiftDir = -1
//
//                            val nextPart = shiftPartition(pIdx, shiftDir)
//
//                            val ds2Row = ((DATASET_MARKER_1, nextPart), row._2)
//
//                            if (iter.hasNext)
//                                Iterator(ds2Row)
//                            else
//                                Array(((DATASET_MARKER_0, rTreePartIdx), (rTree, 0.0, null, null)), ds2Row).iterator
//                        }
//                    })
//                        .filter(_ != null)
//                        .flatMap(_.seq)
//                }, true)
//
//            if (i + 1 < maxIterations)
//                rdd = rdd.repartitionAndSortWithinPartitions(partitionerInPlace)
//        })
//        //        println(">> " + rdd.mapPartitions(_.filter(_._1._1 == DATASET_MARKER_0)).count)
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
//                Iterator((row._2._3, row._2._4))
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
//        //        val itemDist = new ItemDistance() with Serializable {
//        //            override def distance(item1: ItemBoundable, item2: ItemBoundable) = {
//        //
//        //                val jtsGeomFact = new GeometryFactory
//        //                item1.getItem.asInstanceOf[(GMGeomBase, Set[(Double, GMGeomBase)])]._1.toJTS(jtsGeomFact)(0).distance(
//        //                    item2.getItem.asInstanceOf[Point])
//        //            }
//        //        }
//        //
//        //        val startTime = System.currentTimeMillis()
//        //
//        //        val rddPlain1 = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)
//        //        val numParts = rddPlain1.getNumPartitions
//        //
//        //        val countPerPartition = sc.runJob(rddPlain1, getIteratorSize _, 0 until 1).sum.toInt // rddPlain1.mapPartitionsWithIndex((pIdx, iter) => Iterator(if (pIdx == 0) iter.map(x => 1).sum else 0)).first()
//        //
//        //        val partitionerHilbertCluster = new Partitioner() {
//        //            override def numPartitions = numParts
//        //            private def maxIdxCount = (hilbertSize / numPartitions) + 1
//        //            override def getPartition(key: Any): Int = key match { case k: Int => k / maxIdxCount }
//        //        }
//        //
//        //        val partitionerBalanced = new Partitioner() {
//        //            override def numPartitions = numParts
//        //            override def getPartition(key: Any): Int = key match { case k: Long => k.toInt / countPerPartition % numPartitions }
//        //        }
//        //
//        //        val partitionerShift = new Partitioner() {
//        //            override def numPartitions = numParts
//        //            override def getPartition(key: Any): Int = {
//        //
//        //                val (datasetMarker, pIdx) = key.asInstanceOf[(Byte, Int)]
//        //
//        //                if (datasetMarker == DATASET_MARKER_1)
//        //                    (pIdx + 1) % numPartitions
//        //                else
//        //                    pIdx
//        //            }
//        //        }
//        //
//        //        val rdd1 = getRDD(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj), minPartitions, hilbertSize, hilbertBoxWidth, hilbertBoxHeight, partitionerHilbertCluster, partitionerBalanced)
//        //            .mapPartitionsWithIndex((pIdx, iter) => {
//        //
//        //                val rTree = new STRtree
//        //                val jtsGeomFact = new GeometryFactory
//        //
//        //                iter.foreach(row => {
//        //
//        //                    val env = row._2.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//        //                    env.expandBy(errorRange)
//        //
//        //                    rTree.insert(env, (row._2, SortedSet[(Double, GMGeomBase)]()))
//        //                })
//        //
//        //                val geom: GMGeomBase = null
//        //                val sSet: SortedSet[(Double, GMGeomBase)] = null
//        //
//        //                Iterator(((DATASET_MARKER_0, pIdx), (rTree, geom, sSet)))
//        //            }, true)
//        //            //            .cache()
//        //            .partitionBy(partitionerShift)
//        //
//        //        val rdd2 = getRDD(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj), minPartitions, hilbertSize, hilbertBoxWidth, hilbertBoxHeight, partitionerHilbertCluster, partitionerBalanced)
//        //            .mapPartitionsWithIndex((pIdx, iter) => {
//        //
//        //                iter.map(row => {
//        //
//        //                    val rTree: STRtree = null
//        //
//        //                    ((DATASET_MARKER_1, (numParts + pIdx - 2) % numParts), (rTree, row._2, SortedSet[(Double, GMGeomBase)]()))
//        //                })
//        //            }, true)
//        //            //            .cache()
//        //            .partitionBy(partitionerShift)
//        //
//        //        val rdd = rdd1.union(rdd2)
//        //
//        //        sc.parallelize(0 until numParts, numParts)
//        //            .mapPartitionsWithIndex((pIdx, iter) => {
//        //
//        //                val arr = rdd.context.runJob(rdd, getCount _)
//        //                arr.iterator
//        //            })
//        //            .saveAsTextFile(outDir, classOf[GzipCodec])
//
//        //        val rdd = rdd1.union(rdd2)
//        //
//        //        rdd.mapPartitionsWithIndex((pIdx, iter) => {
//        //
//        //            iter.map(row => {
//        //
//        //                val arr = rdd.context.runJob(rdd, getCount _)
//        //                arr.foreach(println)
//        //                row
//        //            })
//        //        }).saveAsTextFile(outDir, classOf[GzipCodec])
//
//        //        var rdd = rdd1.union(rdd2)
//        //
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
//        //
//        //        rdd.mapPartitions(_.map(row => {
//        //
//        //            if (row._1._1 == DATASET_MARKER_0) {
//        //
//        //                val lst = ListBuffer[(GMGeomBase, Set[(Double, GMGeomBase)])]()
//        //
//        //                getTreeItems(row._2._1.getRoot, lst)
//        //
//        //                lst.iterator
//        //            }
//        //            else
//        //                Iterator((row._2._2, row._2._3))
//        //        }))
//        //            .flatMap(_.seq)
//        //            .mapPartitions(_.map(row => {
//        //
//        //                val sb = StringBuilder.newBuilder
//        //                    .append(row._1.payload)
//        //                    .append(",[")
//        //
//        //                row._2.foreach(matches => {
//        //                    sb.append("(")
//        //                        .append(matches._1)
//        //                        .append(",")
//        //                        .append(matches._2.payload)
//        //                        .append("),")
//        //                })
//        //                sb.append("]")
//        //                    .toString()
//        //            }))
//        //            .saveAsTextFile(outDir, classOf[GzipCodec])
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
//
//    def getCount(iter: Iterator[_]): Long = {
//        -999
//    }
//}