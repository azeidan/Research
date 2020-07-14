//package org.cusp.bdi.sknn
//
//import scala.collection.mutable.HashMap
//import scala.collection.mutable.ListBuffer
//import scala.collection.mutable.Set
//
//import org.apache.spark.Partitioner
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.serializer.KryoSerializer
//import org.cusp.bdi.gm.GeoMatch
//import org.cusp.bdi.gm.geom.GMGeomBase
//import org.cusp.bdi.util.Helper
//import org.locationtech.jts.geom.GeometryFactory
//import org.locationtech.jts.geom.Point
//import org.locationtech.jts.index.strtree.AbstractNode
//import org.locationtech.jts.index.strtree.ItemBoundable
//import org.locationtech.jts.index.strtree.ItemDistance
//import org.cusp.bdi.gm.geom.GMPoint
//import org.cusp.bdi.util.HilbertIndex_V2
//import org.apache.hadoop.io.compress.GzipCodec
//import scala.collection.mutable.Queue
//import scala.collection.mutable.SortedSet
//import scala.collection.mutable.HashSet
//import scala.collection.mutable.TreeSet
//import org.cusp.bdi.util.HilbertIndex
//import java.io.PrintWriter
//import java.io.File
//import org.locationtech.jts.index.strtree.STRtree
//
////abstract class GMGeomBaseKNN(_gmGeomBase: GMGeomBase) extends Serializable with Ordered[GMGeomBaseKNN] {
////    def gmGeomBase = _gmGeomBase
////    override def compare(other: GMGeomBaseKNN) = gmGeomBase.compare(other.gmGeomBase)
////    override def equals(other: Any) = gmGeomBase.equals(other match { case x: GMGeomBaseKNN => x.gmGeomBase })
////
////}
////case class GMGeomBaseKNN1(_gmGeomBase: GMGeomBase) extends GMGeomBaseKNN(_gmGeomBase) {}
////case class GMGeomBaseKNN2(_gmGeomBase: GMGeomBase) extends GMGeomBaseKNN(_gmGeomBase) {}
//
//object SparkKNN {
//
//    private val DATASET_MARKER_0: Byte = 0
//    private val DATASET_MARKER_1: Byte = 1
//
//    //    def getRDD(sc: SparkContext, fileName: String, objType: String, isDataset1: Boolean, minPartitions: Int, hilbertSize: Int, hilbertBoxWidth: Int, hilbertBoxHeight: Int, partitionerHilbertCluster: Partitioner) = {
//
//    //        RDD_Store.getRDD(sc, fileName, objType, minPartitions)
//    //            .mapPartitionsWithIndex((pIdx, iter) => {
//    //
//    //                iter.map(row => {
//    //
//    //                    val pointCoords = (row._2._1.toInt, row._2._2.toInt)
//    //
//    //                    val hIdx = HilbertIndex.computeIndex(hilbertSize, (pointCoords._1 / hilbertBoxWidth, pointCoords._2 / hilbertBoxHeight))
//    //
//    //                    val gmGeom: GMGeomBase = new GMPoint(row._2._1, pointCoords)
//    //
//    //                    if (isDataset1)
//    //                        ((DATASET_MARKER_0, hIdx), gmGeom)
//    //                    else
//    //                        ((DATASET_MARKER_1, hIdx), gmGeom)
//    //                })
//    //            })
//    //            .partitionBy(partitionerHilbertCluster)
//
//    //        rdd = dataSetMarker match {
//    //            case DATASET_MARKER_0 => rdd.repartitionAndSortWithinPartitions(partitionerDistance)
//    //            case DATASET_MARKER_1 => rdd.partitionBy(partitionerDistance)
//    //        }
//    //        rdd .mapPartitions(_.map(_._2)) // remove distance
//    //            .zipWithIndex() // enumerate records
//    //            .mapPartitions(_.map(row => (row._2, row._1))) // make row ID the key
//    //            .partitionBy(partitionerEquaCount) // equal-count partitioning
//
//    //        rdd1 = dataSetMarker match {
//    //            case DATASET_MARKER_0 => rdd1.mapPartitionsWithIndex(f, preservesPartitioning)
//    //            case DATASET_MARKER_1 => rdd.partitionBy(partitionerDistance)
//    //        }
//    //
//    //        rdd.mapPartitions(_.map(row => (dataSetMarker, row._2))) // remove the key
//    //        RDD_Store.getRDD(sc, fileName, objType, minPartitions)
//    //            .mapPartitions(_.map(row => {
//    //
//    //                val pointCoords = (row._2._1.toInt, row._2._2.toInt)
//    //
//    //                val hIdx = HilbertIndex.computeIndex(hilbertSize, (pointCoords._1 / hilbertBoxWidth, pointCoords._2 / hilbertBoxHeight))
//    //
//    //                val gmGeom: GMGeomBase = new GMPoint(row._1, pointCoords)
//    //
//    //                (hIdx, gmGeom)
//    //            }))
//    //            .repartitionAndSortWithinPartitions(partitionerHilbertCluster)
//    //            .zipWithIndex()
//    //            .mapPartitions(_.map(row => (row._2, row._1._2)))
//    //            .partitionBy(partitionerBalanced)
//    //    }
//
//    def main(args: Array[String]): Unit = {
//        //        //(935,1477)
//        //        println(HilbertIndex_V2.computeIndex(Math.pow(2, 15).toInt, (935,1477)))
//        //        println(HilbertIndex_V2.getNearbyIndexes(Math.pow(2, 15).toInt, 1842220))
//        //        //(934,1476)
//        //        println(HilbertIndex_V2.computeIndex(Math.pow(2, 15).toInt, (934,1476)))
//        //        println(HilbertIndex_V2.getNearbyIndexes(Math.pow(2, 15).toInt, 1842222))
//        //        System.exit(0)
//        //        val max = Math.pow(2, 15).toInt
//        //        val stopAt = max - 1
//
//        //                println(HilbertIndex.computeIndex(31, (0, 48)))
//
//        //        val pw = new PrintWriter(new File("/media/ayman/Data/GeoMatch_Files/OutputFiles/hIdx.csv" ))
//
//        //println(HilbertIndex_V2.computeIndex(max, (0, 2)))
//        //        (stopAt to 0 by -1).foreach(x => {
//        //            (0 to stopAt by 1).foreach(y => {
//        //                //println((x, y))
//        ////                                pw.print(HilbertIndex_V2.computeIndex(max, (x, y)) + "\t")
//        ////                val h = HilbertIndex_V2.computeIndex(max, (x, y))
//        ////println(h)
//        ////                if(h<0)
//        ////                    println()
//        //            })
//        ////            if(x%1000==0)println(x)
//        //        })
//        //pw.flush()
//        //pw.close()
//        //        val max = 31
//        //        val idx0 = HilbertIndex_V2.computeIndex(max, (0, 0))
//        //        val idx1 = HilbertIndex_V2.computeIndex(max, (0, 1))
//        //
//        //        println(idx0)
//        //        println(idx1)
//        //        //
//        //        //        println(HilbertIndex_V2.getNearbyIndexes(max, 0))
//        //        //        println(HilbertIndex_V2.getNearbyIndexes(max, 1))
//        //
//        //        //
//        //                (Math.pow(2, hilbertN).toInt-1 to 0 by -10000).foreach(row => {
//        //                    (0 until Math.pow(2, hilbertN).toInt by 10000).foreach(col => {
//        //                        //println(HilbertIndex.computeIndex(max,(3,2)))
//        //                        //println(HilbertIndex_V2.computeIndex(max,(3,2)))
//        //
//        //                        //println(HilbertIndex.getNearbyIndexes(max,1))
//        ////                        println(HilbertIndex_V2.getNearbyIndexes(hilbertN,512))
//        ////        println(HilbertIndex_V2.computeIndex(max, (16,16)))
//        //                        val h = HilbertIndex.computeIndex(hilbertN, (col % hilbertN, row % hilbertN))
//        //
//        //                        if (h < 0)
//        //                            println(">> " + HilbertIndex_V2.computeIndex(hilbertN, (row % hilbertN, col % hilbertN)))
//        //        //
//        ////                        print(h + "\t")
//        //                    })
//        ////                    println()
//        //                })
//        //        System.exit(0)
//
//        val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift
//        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint
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
//        //        val xMin = 909126.0155
//        //        val xMax = 110626.2880
//        //        val yMin = 1610215.3590
//        //        val yMax = 424498.0529
//
//        val hilbertN = Math.pow(2, 15).toInt
//        //        val hilbertMaxIndex = Math.pow(2, hilbertN).toInt
//        //        val hilbertBoxWidth = 1 //xMax - xMin
//        //        val hilbertBoxHeight = 1 //yMax - yMin
//
//        val startTime = System.currentTimeMillis()
//
//        val rddPlain1 = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)
//        val numParts = rddPlain1.getNumPartitions
//        val maxPartRowCount = (sc.runJob(rddPlain1, getIteratorSize _, Array(0, numParts / 2, numParts - 1)).sum / 3)
//        val maxPartRowCountAdjusted = (maxPartRowCount * 1.1).toInt
//        //        val maxHilbertPerPart = Math.pow(2, 30).toInt / numParts + 1
//        //        val radiusSegment = Int.MaxValue / numParts + 1
//
//        //        val partitionerDistance = new Partitioner() {
//        //            override def numPartitions = numParts
//        //            override def getPartition(key: Any): Int = key match { case k: Double => k.toInt / radiusSegment }
//        //        }
//
//        val partitionerInPlace = new Partitioner() {
//
//            override def numPartitions = numParts
//            override def getPartition(key: Any): Int =
//                key.asInstanceOf[(Byte, Int)]._2
//        }
//
//        val itemDist = new ItemDistance() with Serializable {
//            override def distance(item1: ItemBoundable, item2: ItemBoundable) = {
//
//                val jtsGeomFact = new GeometryFactory
//                item1.getItem.asInstanceOf[GMGeomBase].toJTS(jtsGeomFact)(0).distance(
//                    item2.getItem.asInstanceOf[Point])
//            }
//        }
//
//        //        val partitionerEquaCount = new Partitioner() {
//        //            override def numPartitions = numParts
//        //            override def getPartition(key: Any): Int = key match { case k: Long => k.toInt / maxPartRowCount % numPartitions }
//        //        }
//
//        //        val partitionerHilbertCluster = new Partitioner() {
//        //            override def numPartitions = numParts
//        //            //            private def maxIdxCount = (hilbertMaxIndex / numPartitions) + 1
//        //            override def getPartition(key: Any): Int = key match { case k: Int => k / maxHilbertPerPart }
//        //            //            override def getPartition(key: Any): Int = key.asInstanceOf[(Byte, Int)]._2 / maxPartRowCount
//        //        }
//
//        val rdd0 = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)
//            .mapPartitionsWithIndex((pIdx, iter) => {
//
//                val lineParser = RDD_Store.getParser(clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj))
//
//                iter.map(line => {
//
//                    val lineParts = lineParser(line)
//
//                    val pointCoords = (lineParts._2._1.toInt, lineParts._2._2.toInt)
//
//                    val hIdx = HilbertIndex_V2.computeIndex(hilbertN, (pointCoords._1 % hilbertN, pointCoords._2 % hilbertN))
//
//                    val gmGeom: GMGeomBase = new GMPoint(lineParts._1.substring(0, lineParts._1.length - lineParts._2._1.length - lineParts._2._2.length - 2), (lineParts._2._1.toInt, lineParts._2._2.toInt))
//
//                    (hIdx, gmGeom)
//                })
//            })
//            .persist()
//
//        val arrIdxCounts = rdd0.mapPartitions(iter => {
//
//            val mapIdxCounts = HashMap[Int, Long]()
//
//            while (iter.hasNext) {
//
//                val idx = iter.next._1
//
//                val count = mapIdxCounts.get(idx)
//
//                if (count == None)
//                    mapIdxCounts.put(idx, 1L)
//                else
//                    mapIdxCounts.update(idx, count.get + 1)
//            }
//
//            mapIdxCounts.iterator
//        }) // (hIdx, count)
//            .reduceByKey(_ + _)
//            .collect()
//        //            .map(row => {
//        //
//        //                val xyPoint = HilbertIndex_V2.reverseIndex(hilbertN, row._1)
//        //
//        //                (row._1, xyPoint._1 + xyPoint._2, row._2) // Manhattan distance from point of origin (0,0)
//        //            })
//        //            .sortBy(_._2) // by Manhattan dist
//
//        val hIdx = arrIdxCounts.minBy(_._1)
//
//        var partCounter = 0
//        var partLoad = 0L
//        val mapHIdxPart = HashMap[Int, Int]()
//        //        arrIdxCounts.foreach(x => println(">>arrIdxCounts>> " + x))
//        val iter = arrIdxCounts.iterator
//
//        while (iter.hasNext) {
//
//            val (hIdx, mDist, hIdxCount) = iter.next
//
//            if ((partLoad + hIdxCount) > maxPartRowCountAdjusted)
//                if (partLoad > 0) {
//                    partCounter += 1
//                    partLoad = 0
//                }
//
//            mapHIdxPart.put(hIdx, partCounter)
//            partLoad += hIdxCount
//        }
//
//        //                mapHIdxPart.foreach(x => println(">>mapHIdxPart>> " + x))
//        //                System.exit(0)
//
//        val partitionerLookup = new Partitioner() {
//            override def numPartitions = numParts
//            override def getPartition(key: Any): Int = key match {
//                case k: Int => {
//
//                    val partNum = mapHIdxPart.get(k)
//
//                    if (partNum == None)
//                        numParts - 1
//                    else
//                        partNum.get
//                }
//            }
//        }
//
//        var rdd1 = rdd0
//            .partitionBy(partitionerLookup)
//            .mapPartitions(iter => {
//
//                val rTree = new STRtree
//                val jtsGeomFact = new GeometryFactory
//                var hIdx = -1
//                var geomInfo: (Any, Any) = null
//
//                while (iter.hasNext) {
//
//                    val row = iter.next()
//
//                    val env = row._2.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//                    env.expandBy(errorRange)
//
//                    rTree.insert(env, row._2)
//
//                    if (!iter.hasNext)
//                        hIdx = row._1
//                }
//
//                Iterator((hIdx, (DATASET_MARKER_0, rTree, geomInfo)))
//            }, true)
//            .partitionBy(partitionerLookup)
//
//        rdd0.unpersist()
//
//        rdd1 = rdd1.persist()
//        //        rdd1.mapPartitionsWithIndex((p, i) => i.map(x => (p, x))).distinct().foreach(x => println("1>>" + x))
//        //        System.exit(0)
//        val rdd2 = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), minPartitions)
//            .mapPartitionsWithIndex((pIdx, iter) => {
//
//                val lineParser = RDD_Store.getParser(clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj))
//                val rTree: STRtree = null
//
//                iter.map(line => {
//
//                    val lineParts = lineParser(line)
//
//                    val pointCoords = (lineParts._2._1.toInt, lineParts._2._2.toInt)
//
//                    val hIdx = HilbertIndex_V2.computeIndex(hilbertN, (pointCoords._1 % hilbertN, pointCoords._2 % hilbertN))
//
//                    val gmGeom: GMGeomBase = new GMPoint(lineParts._1.substring(0, lineParts._1.length - lineParts._2._1.length - lineParts._2._2.length - 2), (lineParts._2._1.toInt, lineParts._2._2.toInt))
//
//                    val geomInfo: (Any, Any) = (gmGeom, SortedSet[(Double, GMGeomBase)]())
//
//                    (hIdx, (DATASET_MARKER_1, rTree, geomInfo))
//                })
//            })
//            .partitionBy(partitionerLookup)
//        //        rdd2.mapPartitionsWithIndex((p, i) => i.map(x => (p, x))).distinct().foreach(x => println("2>>" + x))
//        //        System.exit(0)
//        var rdd = rdd1.union(rdd2)
//            .mapPartitionsWithIndex((pIdx, iter) => {
//
//                var rTree: STRtree = null
//                var geomInfo: (Any, Any) = null
//                var rTreeHIdx = -1
//                val jtsGeomFact = new GeometryFactory
//                var flag = true
//                var shiftCountUp = 0
//                var shiftCountDown = 0
//
//                iter.map(row => {
//                    val xx = pIdx
//                    if (row._2._1 == DATASET_MARKER_0) {
//
//                        rTree = row._2._2
//                        rTreeHIdx = row._1
//
//                        if (iter.hasNext) // when DS2 is not present at this partition
//                            null
//                        else
//                            Iterator(row)
//                    }
//                    else {
//
//                        if (rTree != null && rTree.getRoot.size() > 0) {
//
//                            val (gmGeomBase, geomSortSet): (GMGeomBase, Set[(Double, GMGeomBase)]) = row._2._3.asInstanceOf[(GMGeomBase, Set[(Double, GMGeomBase)])]
//                            val jtsGeom = gmGeomBase.toJTS(jtsGeomFact)(0)
//                            val jtsEnv = jtsGeom.getEnvelopeInternal
//
//                            jtsEnv.expandBy(errorRange)
//
//                            import scala.collection.JavaConversions._
//
//                            rTree.nearestNeighbour(jtsEnv, jtsGeom, itemDist, kParam)
//                                .foreach(treeMatch => {
//
//                                    val matchGeom = treeMatch match { case x: GMGeomBase => x }
//
//                                    // if (matchGeom.payload.startsWith("2014-09-08 02:09:22,1002731.2265481339,231911.84350943242,m15,5606,m150316,1,401738,0,95.12,85.11"))
//                                    //     println()
//
//                                    val dist = matchGeom.toJTS(jtsGeomFact)(0).distance(jtsGeom)
//
//                                    geomSortSet.add((dist, matchGeom))
//
//                                    while (geomSortSet.size > kParam) geomSortSet.remove(geomSortSet.last)
//                                })
//                        }
//
//                        val ds2Row = (row._1, row._2)
//
//                        if (iter.hasNext)
//                            Iterator(ds2Row)
//                        else
//                            Iterator((rTreeHIdx, (DATASET_MARKER_0, rTree, geomInfo)), ds2Row)
//                    }
//                })
//                    .filter(_ != null)
//                    .flatMap(_.seq)
//            } /*, true*/ )
//
//        rdd.mapPartitions(_.map(row => {
//
//            if (row._2._2 != null) {
//
//                val lst = ListBuffer[GMGeomBase]()
//
//                getTreeItems(row._2._2.getRoot, lst)
//
//                lst.map(x => (x, null))
//            }
//            else
//                Iterator(row._2._3.asInstanceOf[(GMGeomBase, Set[(Double, GMGeomBase)])])
//        }))
//            .flatMap(_.seq)
//            .mapPartitions(_.map(row => {
//                //
//                val sb = StringBuilder.newBuilder
//                    .append(row._1.payload)
//                    .append(",[")
//
//                if (row._2 != null)
//                    row._2.foreach(matches => {
//                        sb.append("(")
//                            .append(matches._1)
//                            .append(",")
//                            .append(matches._2.payload)
//                            .append("),")
//                    })
//                sb.append("]")
//                    .toString()
//            }))
//            .saveAsTextFile(outDir, classOf[GzipCodec])
//
//        //        println(mapPartIdxDist.map(_._1._2).sum)
//        //        mapPartIdxDist.foreach(x => println("@" + x))
//        //        System.exit(0)
//        //        var nextIdxPointer = 0
//
//        //        while(
//
//        //        val maxPartRowCountAdjusted = ((arrHIndexCounts.map(_._2).sum * 1.1) / numParts).toInt
//        //        println(arrHIndexCounts.size)
//        //        val l = arrHIndexCounts.map(row => (row._1, HilbertIndex_V2.getNearbyIndexes(hilbertN, row._1).filter(_ > row._1)))
//        //
//        //        l.foreach(x => println(s">>$x"))
//
//        //        var partCounter = 0
//        //        var sum = 0L
//        //        val mapPartIdxDist = HashMap[Int, TreeSet[Int]]()
//        //
//        //        var arrIdx = 0
//        //        var hIdx = arrHIndexCounts(arrIdx)._1
//        //        var setProcessedIdex = HashSet[Int]()
//        //
//        //        val hSet = TreeSet[Int]()
//        //
//        //        mapPartIdxDist.put(partCounter, hSet)
//        //
//        //        while (arrIdx < arrHIndexCounts.length) {
//        //
//        //            var lstIdxNeighbor = HilbertIndex_V2.getNearbyIndexes(hilbertN, hIdx)
//        //            arrIdx += 1
//        //            hIdx = arrHIndexCounts(arrIdx)._1
//        //
//        //            var lstIdx = 0
//        //            while (lstIdx < lstIdxNeighbor.size) {
//        //
//        //                val n = lstIdxNeighbor(lstIdx)
//        //
//        //                if (n == arrHIndexCounts(arrIdx)) {
//        //
//        //                }
//        //                else
//        //                    HilbertIndex_V2.getNearbyIndexes(hilbertN, n).foreach(x => lstIdxNeighbor.append(x))
//        //            }
//        //
//        //            val hIdxCount = arrHIndexCounts.get(hIdx).getOrElse(0L)
//        //
//        //            if ((sum + hIdxCount) > maxPartRowCountAdjusted) {
//        //
//        //                mapPartIdxDist.put(idxCount._1, partCounter)
//        //                partCounter += 1
//        //                sum = 0
//        //            }
//        //            else {
//        //
//        //                hSet.add(hIdx)
//        //                setProcessedIdex.app
//        //                hIdx
//        //            }
//        //
//        //            if (partCounter == 11)
//        //                print()
//        //            hIdx += 1
//        //        }
//        //        mapPartIdxDist.foreach(x => println(s">>$x"))
//        //        System.exit(0)
//        //        val rdd1 = rdd0
//        //            .mapPartitions(iter => {
//        //
//        //                iter.map(row => {
//        //
//        //                    val gmGeom: GMGeomBase = new GMPoint(row._2._1, (row._2._2._1.toInt, row._2._2._2.toInt))
//        //
//        //                    ((DATASET_MARKER_0, row._1), gmGeom)
//        //                })
//        //            })
//        //            .partitionBy(partitionerHilbertCluster)
//        //
//        //        rdd0.unpersist()
//        //
//        //        val rdd2 = RDD_Store.getRDD(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj), minPartitions)
//        //            .mapPartitionsWithIndex((pIdx, iter) => {
//        //
//        //                iter.map(row => {
//        //
//        //                    val pointCoords = (row._2._1.toInt, row._2._2.toInt)
//        //
//        //                    val hIdx = HilbertIndex.computeIndex(hilbertSize, (pointCoords._1, pointCoords._2))
//        //
//        //                    val gmGeom: GMGeomBase = new GMPoint(row._2._1, pointCoords)
//        //
//        //                    ((DATASET_MARKER_1, hIdx), gmGeom)
//        //                })
//        //            })
//        //            .partitionBy(partitionerHilbertCluster)
//        //
//        //        val rdd = rdd1.union(rdd2)
//        //            .mapPartitions(iter => {
//        //
//        //                val rTree = new STRtree
//        //                val jtsGeomFact = new GeometryFactory
//        //
//        //                iter.map(row => {
//        //
//        //                    var geomSortSet: SortedSet[(Double, GMGeomBase)] = null
//        //
//        //                    if (row._1._1 == DATASET_MARKER_0) {
//        //
//        //                        val jtsEnv = row._2.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//        //                        jtsEnv.expandBy(errorRange)
//        //
//        //                        rTree.insert(jtsEnv, (row._2, SortedSet[(Double, GMGeomBase)]()))
//        //                    }
//        //                    else {
//        //
//        //                        geomSortSet = SortedSet[(Double, GMGeomBase)]()
//        //
//        //                        if (rTree.getRoot != null) {
//        //
//        //                            val jtsGeom = row._2.toJTS(jtsGeomFact)(0)
//        //                            val jtsEnv = jtsGeom.getEnvelopeInternal
//        //                            jtsEnv.expandBy(errorRange)
//        //
//        //                            import scala.collection.JavaConversions._
//        //
//        //                            rTree.nearestNeighbour(jtsEnv, jtsGeom, itemDist, kParam)
//        //                                .foreach(treeMatch => {
//        //
//        //                                    val (rTreeMatchGeom, rTreeMatchSorSet) = treeMatch.asInstanceOf[(GMGeomBase, SortedSet[(Double, GMGeomBase)])]
//        //
//        //                                    // if (matchGeom.payload.startsWith("2014-09-08 02:09:22,1002731.2265481339,231911.84350943242,m15,5606,m150316,1,401738,0,95.12,85.11"))
//        //                                    //     println()
//        //
//        //                                    if (rTreeMatchSorSet.find(x => x._2.equals(row._2)) == None) {
//        //
//        //                                        val dist = rTreeMatchGeom.toJTS(jtsGeomFact)(0).distance(jtsGeom)
//        //
//        //                                        geomSortSet.add((dist, rTreeMatchGeom))
//        //                                        rTreeMatchSorSet.add((dist, row._2))
//        //
//        //                                        while (geomSortSet.size > kParam) geomSortSet.remove(geomSortSet.last)
//        //                                        while (rTreeMatchSorSet.size > kParam) rTreeMatchSorSet.remove(rTreeMatchSorSet.last)
//        //                                    }
//        //                                })
//        //                        }
//        //                    }
//        //
//        //                    if (geomSortSet == null)
//        //                        null
//        //                    else if (iter.hasNext)
//        //                        Iterator((null, row._2, geomSortSet))
//        //                    else
//        //                        Iterator((null, row._2, geomSortSet), (rTree, null, null))
//        //                })
//        //                    .filter(_ != null)
//        //                    .flatMap(_.seq)
//        //            })
//        //
//
//        //            .saveAsTextFile(outDir, classOf[GzipCodec])
//        //
//        //        //        mapPartIdxDist.put(hilbertSize, partCounter + 1)
//        //        //        mapPartIdxDist.foreach(x => println(s">> $x"))
//        //
//        //        //        .mapPartitionsWithIndex((pIdx, iter) => {
//        //        //            var count = 0
//        //        //            while (iter.hasNext) {
//        //        //                count += 1
//        //        //                iter.next()
//        //        //            }
//        //        //
//        //        //            Iterator((pIdx, count))
//        //        //        }).collect()
//        //
//        //        //        val rdd2 = getRDD(sc, /*DATASET_MARKER_1,*/ clArgs.getParamValueString(SparkKNN_Arguments.secondSet), clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj), minPartitions, partitionerDistance)
//        //
//        //        //        rdd2.mapPartitionsWithIndex((oIdx, iter) => {
//        //        //
//        //        //            iter.map(row => {
//        //        //
//        //        //                //                val c = rdd1.context.runJob(rdd1, getIteratorSize _, 0 until 1).sum.toInt
//        //        //
//        //        //                val rdd = rdd2.partitionBy(partitioner)
//        //        //
//        //        //                println(rdd)
//        //        //
//        //        //                row
//        //        //            })
//        //        //        }).collect
//        //        //        val partitionerShift = new Partitioner() {
//        //        //            override def numPartitions = numParts
//        //        //            override def getPartition(key: Any): Int = {
//        //        //
//        //        //                val (datasetMarker, pIdx) = key.asInstanceOf[(Byte, Int)]
//        //        //
//        //        //                if (datasetMarker == DATASET_MARKER_1)
//        //        //                    (pIdx + 1) % numPartitions
//        //        //                else
//        //        //                    pIdx
//        //        //            }
//        //        //        }
//        //
//        //        //        def shiftPartitionDown(pIdx: Int) = (numParts + pIdx - 1) % numParts
//        //        //        def shiftPartitionUp(pIdx: Int) = (pIdx + 1) % numParts
//        //
//        //        //        def shiftPartition(pIdx: Int, shiftBy: Int) =
//        //        //            if (pIdx == 0)
//        //        //                1
//        //        //            else
//        //        //                (numParts + pIdx + shiftBy) % numParts
//        //
//        //        //        val rdd1 = getRDD(sc, /*DATASET_MARKER_0,*/ clArgs.getParamValueString(SparkKNN_Arguments.firstSet), clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj), minPartitions, partitionerDistance)
//        //        //            .mapPartitions(_.map(_._2)) // remove distance
//        //        //            .zipWithIndex() // enumerate records
//        //        //            .mapPartitions(_.map(row => (row._2, row._1))) // make row ID the key
//        //        //            .partitionBy(partitionerEquaCount) // equal-count partitioning
//        //        //            .mapPartitionsWithIndex((pIdx, iter) => {
//        //        //
//        //        //                val rTree = new STRtree
//        //        //                val jtsGeomFact = new GeometryFactory
//        //        //
//        //        //                iter.foreach(row => {
//        //        //
//        //        //                    val env = row._2.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//        //        //                    env.expandBy(errorRange)
//        //        //
//        //        //                    rTree.insert(env, (row._2, SortedSet[(Double, GMGeomBase)]()))
//        //        //                })
//        //        //
//        //        //                val geom: GMGeomBase = null
//        //        //                val sSet: SortedSet[(Double, GMGeomBase)] = null
//        //        //
//        //        //                Iterator(((DATASET_MARKER_0, pIdx), (rTree, 0.0, geom, sSet)))
//        //        //            }, true)
//        //        //            //            .partitionBy(partitionerShift)
//        //        //            .partitionBy(partitionerInPlace)
//        //
//        //        //        rdd2.saveAsTextFile(outDir, classOf[GzipCodec])
//        //        //var rdd2 = getRDD(sc, /*DATASET_MARKER_1,*/ clArgs.getParamValueString(SparkKNN_Arguments.secondSet), clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj), minPartitions, partitionerDistance)
//        //        //            .zipWithIndex() // enumerate records
//        //        //            .mapPartitions(_.map(row => (row._2, row._1))) // make row ID the key
//        //        //            .partitionBy(partitionerEquaCount)
//        //        //            .mapPartitionsWithIndex((pIdx, iter) => {
//        //        //
//        //        //                iter.map(row => {
//        //        //
//        //        //                    val rTree: STRtree = null
//        //        //
//        //        //                    ((DATASET_MARKER_1, pIdx), (rTree, row._2._1, row._2._2, SortedSet[(Double, GMGeomBase)]()))
//        //        //                })
//        //        //            }, true)
//        //        //            .partitionBy(partitionerInPlace)
//        //        //            .partitionBy(partitionerShift)
//        //
//        //        //        var rdd = rdd1.union(rdd2)
//        //        //
//        //        //        val maxIterations = 2 //numberOfIterations
//        //        //
//        //        //        (0 until maxIterations).foreach(i => {
//        //        //            rdd = rdd
//
//        //        //        println(">> " + rdd.mapPartitions(_.filter(_._1._1 == DATASET_MARKER_0)).count)
//        //        //        System.exit(0)
//        //        //        rdd.mapPartitions(_.map(row => {
//        //        //
//        //        //            if (row._1._1 == DATASET_MARKER_0) {
//        //        //
//        //        //                val lst = ListBuffer[(GMGeomBase, Set[(Double, GMGeomBase)])]()
//        //        //
//        //        //                getTreeItems(row._2._1.getRoot, lst)
//        //        //
//        //        //                lst.iterator
//        //        //            }
//        //        //            else
//        //        //                Iterator((row._2._3, row._2._4))
//        //        //        }))
//        //        //            .flatMap(_.seq)
//        //        //            .mapPartitions(_.map(row => {
//        //        //
//        //        //                val sb = StringBuilder.newBuilder
//        //        //                    .append(row._1.payload)
//        //        //                    .append(",[")
//        //        //
//        //        //                row._2.foreach(matches => {
//        //        //                    sb.append("(")
//        //        //                        .append(matches._1)
//        //        //                        .append(",")
//        //        //                        .append(matches._2.payload)
//        //        //                        .append("),")
//        //        //                })
//        //        //                sb.append("]")
//        //        //                    .toString()
//        //        //            }))
//        //        //            .saveAsTextFile(outDir, classOf[GzipCodec])
//        //
//        //        //        val itemDist = new ItemDistance() with Serializable {
//        //        //            override def distance(item1: ItemBoundable, item2: ItemBoundable) = {
//        //        //
//        //        //                val jtsGeomFact = new GeometryFactory
//        //        //                item1.getItem.asInstanceOf[(GMGeomBase, Set[(Double, GMGeomBase)])]._1.toJTS(jtsGeomFact)(0).distance(
//        //        //                    item2.getItem.asInstanceOf[Point])
//        //        //            }
//        //        //        }
//        //        //
//        //        //        val startTime = System.currentTimeMillis()
//        //        //
//        //        //        val rddPlain1 = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)
//        //        //        val numParts = rddPlain1.getNumPartitions
//        //        //
//        //        //        val countPerPartition = sc.runJob(rddPlain1, getIteratorSize _, 0 until 1).sum.toInt // rddPlain1.mapPartitionsWithIndex((pIdx, iter) => Iterator(if (pIdx == 0) iter.map(x => 1).sum else 0)).first()
//        //        //
//        //        //        val partitionerHilbertCluster = new Partitioner() {
//        //        //            override def numPartitions = numParts
//        //        //            private def maxIdxCount = (hilbertSize / numPartitions) + 1
//        //        //            override def getPartition(key: Any): Int = key match { case k: Int => k / maxIdxCount }
//        //        //        }
//        //        //
//        //        //        val partitionerBalanced = new Partitioner() {
//        //        //            override def numPartitions = numParts
//        //        //            override def getPartition(key: Any): Int = key match { case k: Long => k.toInt / countPerPartition % numPartitions }
//        //        //        }
//        //        //
//        //        //        val partitionerShift = new Partitioner() {
//        //        //            override def numPartitions = numParts
//        //        //            override def getPartition(key: Any): Int = {
//        //        //
//        //        //                val (datasetMarker, pIdx) = key.asInstanceOf[(Byte, Int)]
//        //        //
//        //        //                if (datasetMarker == DATASET_MARKER_1)
//        //        //                    (pIdx + 1) % numPartitions
//        //        //                else
//        //        //                    pIdx
//        //        //            }
//        //        //        }
//        //        //
//        //        //        val rdd1 = getRDD(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj), minPartitions, hilbertSize, hilbertBoxWidth, hilbertBoxHeight, partitionerHilbertCluster, partitionerBalanced)
//        //        //            .mapPartitionsWithIndex((pIdx, iter) => {
//        //        //
//        //        //                val rTree = new STRtree
//        //        //                val jtsGeomFact = new GeometryFactory
//        //        //
//        //        //                iter.foreach(row => {
//        //        //
//        //        //                    val env = row._2.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//        //        //                    env.expandBy(errorRange)
//        //        //
//        //        //                    rTree.insert(env, (row._2, SortedSet[(Double, GMGeomBase)]()))
//        //        //                })
//        //        //
//        //        //                val geom: GMGeomBase = null
//        //        //                val sSet: SortedSet[(Double, GMGeomBase)] = null
//        //        //
//        //        //                Iterator(((DATASET_MARKER_0, pIdx), (rTree, geom, sSet)))
//        //        //            }, true)
//        //        //            //            .cache()
//        //        //            .partitionBy(partitionerShift)
//        //        //
//        //        //        val rdd2 = getRDD(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj), minPartitions, hilbertSize, hilbertBoxWidth, hilbertBoxHeight, partitionerHilbertCluster, partitionerBalanced)
//        //        //            .mapPartitionsWithIndex((pIdx, iter) => {
//        //        //
//        //        //                iter.map(row => {
//        //        //
//        //        //                    val rTree: STRtree = null
//        //        //
//        //        //                    ((DATASET_MARKER_1, (numParts + pIdx - 2) % numParts), (rTree, row._2, SortedSet[(Double, GMGeomBase)]()))
//        //        //                })
//        //        //            }, true)
//        //        //            //            .cache()
//        //        //            .partitionBy(partitionerShift)
//        //        //
//        //        //        val rdd = rdd1.union(rdd2)
//        //        //
//        //        //        sc.parallelize(0 until numParts, numParts)
//        //        //            .mapPartitionsWithIndex((pIdx, iter) => {
//        //        //
//        //        //                val arr = rdd.context.runJob(rdd, getCount _)
//        //        //                arr.iterator
//        //        //            })
//        //        //            .saveAsTextFile(outDir, classOf[GzipCodec])
//        //
//        //        //        val rdd = rdd1.union(rdd2)
//        //        //
//        //        //        rdd.mapPartitionsWithIndex((pIdx, iter) => {
//        //        //
//        //        //            iter.map(row => {
//        //        //
//        //        //                val arr = rdd.context.runJob(rdd, getCount _)
//        //        //                arr.foreach(println)
//        //        //                row
//        //        //            })
//        //        //        }).saveAsTextFile(outDir, classOf[GzipCodec])
//        //
//        //        //        var rdd = rdd1.union(rdd2)
//        //        //
//        //        //        val maxIterations = numberOfIterations
//        //        //
//        //        //        (0 until maxIterations).foreach(i => {
//        //        //
//        //        //            rdd = rdd
//        //        //                .mapPartitionsWithIndex((pIdx, iter) => {
//        //        //
//        //        //                    var rTree: STRtree = null
//        //        //                    var rTreePartIdx = -1
//        //        //                    val jtsGeomFact = new GeometryFactory
//        //        //                    var flag = true
//        //        //
//        //        //                    iter.map(row => {
//        //        //
//        //        //                        //                        print(">>%d>> %d $$%s$$%n".format(row._1._1, pIdx,
//        //        //                        //                            if (row._2 == null || row._2._2 == null)
//        //        //                        //                                ""
//        //        //                        //                            else
//        //        //                        //                                row._2._2.payload))
//        //        //
//        //        //                        if (row._1._1 == DATASET_MARKER_0) {
//        //        //
//        //        //                            rTree = row._2._1
//        //        //                            rTreePartIdx = row._1._2
//        //        //                            null
//        //        //                        }
//        //        //                        else {
//        //        //
//        //        //                            if (rTree != null) {
//        //        //
//        //        //                                val geomSortSet: Set[(Double, GMGeomBase)] = row._2._3
//        //        //                                val jtsGeom = row._2._2.toJTS(jtsGeomFact)(0)
//        //        //                                val jtsEnv = jtsGeom.getEnvelopeInternal
//        //        //
//        //        //                                jtsEnv.expandBy(errorRange)
//        //        //
//        //        //                                import scala.collection.JavaConversions._
//        //        //
//        //        //                                rTree.nearestNeighbour(jtsEnv, jtsGeom, itemDist, kParam)
//        //        //                                    .foreach(treeMatch => {
//        //        //
//        //        //                                        val (matchGeom, matchSorSet) = treeMatch.asInstanceOf[(GMGeomBase, Set[(Double, GMGeomBase)])]
//        //        //
//        //        //                                        // if (matchGeom.payload.startsWith("2014-09-08 02:09:22,1002731.2265481339,231911.84350943242,m15,5606,m150316,1,401738,0,95.12,85.11"))
//        //        //                                        //     println()
//        //        //
//        //        //                                        if (matchSorSet.find(x => x._2.equals(row._2._1)) == None) {
//        //        //
//        //        //                                            val dist = matchGeom.toJTS(jtsGeomFact)(0).distance(jtsGeom)
//        //        //
//        //        //                                            geomSortSet.add((dist, matchGeom))
//        //        //                                            matchSorSet.add((dist, row._2._2))
//        //        //
//        //        //                                            while (geomSortSet.size > kParam) geomSortSet.remove(geomSortSet.last)
//        //        //                                            while (matchSorSet.size > kParam) matchSorSet.remove(matchSorSet.last)
//        //        //                                        }
//        //        //                                    })
//        //        //                            }
//        //        //
//        //        //                            val ds2Row = ((DATASET_MARKER_1, pIdx), row._2)
//        //        //
//        //        //                            if (iter.hasNext)
//        //        //                                Iterator(ds2Row)
//        //        //                            else
//        //        //                                Array(((DATASET_MARKER_0, rTreePartIdx), (rTree, null, null)), ds2Row).iterator
//        //        //                        }
//        //        //                    })
//        //        //                        .filter(_ != null)
//        //        //                        .flatMap(_.seq)
//        //        //                }, true)
//        //        //
//        //        //            if (i + 1 < maxIterations)
//        //        //                rdd = rdd.repartitionAndSortWithinPartitions(partitionerShift)
//        //        //        })
//        //        //
//        //        //        rdd.mapPartitions(_.map(row => {
//        //        //
//        //        //            if (row._1._1 == DATASET_MARKER_0) {
//        //        //
//        //        //                val lst = ListBuffer[(GMGeomBase, Set[(Double, GMGeomBase)])]()
//        //        //
//        //        //                getTreeItems(row._2._1.getRoot, lst)
//        //        //
//        //        //                lst.iterator
//        //        //            }
//        //        //            else
//        //        //                Iterator((row._2._2, row._2._3))
//        //        //        }))
//        //        //            .flatMap(_.seq)
//        //        //            .mapPartitions(_.map(row => {
//        //        //
//        //        //                val sb = StringBuilder.newBuilder
//        //        //                    .append(row._1.payload)
//        //        //                    .append(",[")
//        //        //
//        //        //                row._2.foreach(matches => {
//        //        //                    sb.append("(")
//        //        //                        .append(matches._1)
//        //        //                        .append(",")
//        //        //                        .append(matches._2.payload)
//        //        //                        .append("),")
//        //        //                })
//        //        //                sb.append("]")
//        //        //                    .toString()
//        //        //            }))
//        //        //            .saveAsTextFile(outDir, classOf[GzipCodec])
//        //
//        printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
//
//        println(outDir)
//    }
//
//    import scala.collection.JavaConversions._
//    def getTreeItems(node: AbstractNode, lst: ListBuffer[GMGeomBase]) {
//
//        node.getChildBoundables.foreach(item => {
//
//            item match {
//                case an: AbstractNode => getTreeItems(an, lst)
//                case ib: ItemBoundable => lst.append(ib.getItem().asInstanceOf[GMGeomBase])
//            }
//        })
//    }
//
//    def getIteratorSize(iter: Iterator[_]): Long = {
//        var count = 0
//        var size = 0
//        while (iter.hasNext) {
//            count += 1
//            iter.next()
//        }
//        count
//    }
//
//    //    def binarySearch(sortedArr: Array[(Int, Long)], lookupItem: Int): (Int, Long) = {
//    //
//    //        var top = 0
//    //        var bot = sortedArr.length - 1
//    //
//    //        if (lookupItem < sortedArr(top)._1 || lookupItem > sortedArr(bot)._1)
//    //            return null
//    //        else if (lookupItem == sortedArr(top)._1)
//    //            return sortedArr(top)
//    //        else if (lookupItem > sortedArr(bot)._1)
//    //            return sortedArr(bot)
//    //        else
//    //            while (top <= bot) {
//    //
//    //                val mid = top + (bot - top) / 2
//    //
//    //                if (sortedArr(mid)._1 == lookupItem)
//    //                    return sortedArr(mid)
//    //                else if (sortedArr(mid)._1 > lookupItem)
//    //                    bot = mid - 1
//    //                else
//    //                    top = mid + 1
//    //            }
//    //
//    //        return null
//    //    }
//    //
//    //    def getCount(iter: Iterator[_]): Long = {
//    //        -999
//    //    }
//}