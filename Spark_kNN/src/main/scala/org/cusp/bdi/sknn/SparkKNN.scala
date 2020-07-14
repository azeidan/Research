package org.cusp.bdi.sknn

import scala.collection.mutable.ListBuffer
import scala.util.Random

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.sknn.util.AssignToPartitions
import org.cusp.bdi.sknn.util.GridOperation
import org.cusp.bdi.sknn.util.QuadTreeDigestOperations
import org.cusp.bdi.sknn.util.QuadTreeInfo
import org.cusp.bdi.sknn.util.QuadTreeOperations
import org.cusp.bdi.sknn.util.SortSetObj
import org.cusp.bdi.util.Helper

import com.insightfullogic.quad_trees.Box
import com.insightfullogic.quad_trees.Point
import com.insightfullogic.quad_trees.QuadTree
import com.insightfullogic.quad_trees.QuadTreeDigest

case class QTPartId(uniqueIdentifier: Int, totalPoints: Long) {
    var assignedPart = -1

    override def toString() =
        "%d\t%d\t%d".format(assignedPart, uniqueIdentifier, totalPoints)
}

object SparkKNN {

    def getSparkKNNClasses(): Array[Class[_]] =
        Array(classOf[QuadTree],
              classOf[QuadTreeInfo],
              classOf[GridOperation],
              QuadTreeDigestOperations.getClass,
              QuadTreeOperations.getClass,
              Helper.getClass,
              classOf[QuadTreeInfo],
              classOf[SortSetObj],
              classOf[Box],
              classOf[Point],
              classOf[QuadTree],
              classOf[QuadTreeDigest])
}

case class SparkKNN(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int) {

    // for testing, remove ...
    var minPartitions = 0

    def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] =
        knnJoin(rddLeft, rddRight, k).union(knnJoin(rddRight, rddLeft, k))

    def knnJoin(): RDD[(Point, Iterable[(Double, Point)])] =
        knnJoin(rddLeft, rddRight, k)

    private def knnJoin(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int): RDD[(Point, Iterable[(Double, Point)])] = {

        var (execRowCapacity, totalRowCount, mbrDS1) = computeCapacity(rddRight, k)

        //        execRowCapacity = 57702

        //        println(">>" + execRowCapacity)

        var arrHorizDist = computePartitionRanges(rddRight, execRowCapacity)

        val partitionerByX = new Partitioner() {

            override def numPartitions = arrHorizDist.size

            override def getPartition(key: Any): Int =
                key match {
                    case xCoord: Double => binarySearch(arrHorizDist, xCoord.toLong)
                }
        }

        val rddQuadTree = rddRight.mapPartitions(_.map(point => (point.x, point)))
            .repartitionAndSortWithinPartitions(partitionerByX) // places in containers along the x-axis and sort by x-coor
            .mapPartitionsWithIndex((pIdx, iter) => {

                val lstQT = ListBuffer[QuadTreeInfo]()
                var qtInf: QuadTreeInfo = null

                while (iter.hasNext) {

                    val lstPoint = iter.take(execRowCapacity - 1).map(_._2).toList // -1 for the one in var point

                    var (minX, minY, maxX, maxY) = computeMBR(lstPoint)

                    minX = minX.toLong
                    minY = minY.toLong
                    maxX = maxX.toLong + 1
                    maxY = maxY.toLong + 1

                    val halfWidth = (maxX - minX) / 2.0
                    val halfHeight = (maxY - minY) / 2.0

                    qtInf = new QuadTreeInfo(Box(new Point(halfWidth + minX, halfHeight + minY), new Point(halfWidth, halfHeight)))

                    qtInf.quadTree.insert(lstPoint)

                    qtInf.uniqueIdentifier = Random.nextInt()

                    lstQT.append(qtInf)
                }

                //                lstQT.foreach(qtInf => println(">>%s".format(qtInf.toString())))

                lstQT.iterator
            }, true)
            .persist(StorageLevel.MEMORY_ONLY)

        // (box#, Count)
        var arrGridAndQTInf = rddQuadTree
            .mapPartitionsWithIndex((pIdx, iter) => {

                val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

                iter.map(qtInf => {

                    val iterPoint = qtInf.quadTree.getAllPoints.iterator

                    iterPoint.map(point => {

                        //                        if (point.userData.toString().equalsIgnoreCase("taxi_b_918654"))
                        //                            println(gridOp.computeBoxXY(point.xy))

                        if (iterPoint.hasNext)
                            (gridOp.computeBoxXY(point.x, point.y), (1L, Set(qtInf.uniqueIdentifier), null, null))
                        else
                            (gridOp.computeBoxXY(point.x, point.y), (1L, Set(qtInf.uniqueIdentifier), ListBuffer((qtInf.uniqueIdentifier, qtInf.quadTree.getTotalPoints)), ListBuffer(qtInf.quadTree.getMBR)))
                    })
                })
                    .flatMap(_.seq)
            })
            .reduceByKey((x, y) => (x._1 + y._1, x._2 ++ y._2, reducerListBuffer(x._3, y._3), reducerListBuffer(x._4, y._4)))
            .collect

        //        arrGridAndQTInf.foreach(row => "<>\t%d\t%d\t%d".format(row._1._1, row._1._2, row._2._1))

        arrHorizDist = null

        val arrQTmbr = arrGridAndQTInf.map(_._2._4).filter(_ != null).flatMap(_.seq)

        var arrAttrQT = arrGridAndQTInf
            .map(_._2._3)
            .filter(_ != null)
            .flatMap(_.seq)
            .map(row => QTPartId(row._1, row._2))

        val actualPartitionCount = AssignToPartitions(arrAttrQT, execRowCapacity).getPartitionCount

        //        arrAttrQT.foreach(qtdInf => println(">>\t%s".format(qtdInf.toString())))

        val bvMapPartAssign = rddLeft.context.broadcast(arrAttrQT.map(qtPId => (qtPId.uniqueIdentifier, qtPId.assignedPart)).toMap)

        arrAttrQT = null

        val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

        var leftBot = gridOp.computeBoxXY(mbrDS1._1, mbrDS1._2)
        var rightTop = gridOp.computeBoxXY(mbrDS1._3, mbrDS1._4)

        val halfWidth = ((rightTop._1 - leftBot._1).toLong + 1) / 2.0
        val halfHeight = ((rightTop._2 - leftBot._2).toLong + 1) / 2.0

        val qtGlobalIndex = new QuadTreeDigest(Box(new Point(halfWidth + leftBot._1, halfHeight + leftBot._2), new Point(halfWidth, halfHeight)))

        arrGridAndQTInf.foreach(row => qtGlobalIndex.insert((row._1._1, row._1._2), row._2._1, row._2._2))

        //        println(qtGlobalIndex)

        //        mapGridSummary.foreach(x => println(">>\t%d\t%d\t%s".format(x._1, x._2._1, x._2._2.toString)))

        arrGridAndQTInf = null

        //        println(SizeEstimator.estimate(qtGlobalIndex))

        val bvQTGlobalIndex = rddLeft.context.broadcast(qtGlobalIndex)

        var rddPoint = rddLeft
            .mapPartitions(iter => {

                val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

                iter.map(point => {

                    //                    if (point.userData.toString().equalsIgnoreCase("yellow_1_b_548388"))
                    //                        println

                    val lstUId = QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k)
                        .toList

                    //                    if (lstUId.size >= 18)
                    //                        println(QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.xy), k, gridOp.getBoxW, gridOp.getBoxH))

                    val tuple: Any = (point, SortSetObj(k, false), lstUId)

                    (bvMapPartAssign.value.get(lstUId.head).get, tuple)
                })
            })

        //        println("<>" + rddPoint.mapPartitions(_.map(_._2.asInstanceOf[(Point, SortSetObj, List[Int])]._3.size)).max)

        //        println(QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(1013487.21, 134367.52), k))

        val numRounds = arrQTmbr.map(mbr => {

            val lowerLeft = (mbr._1, mbr._2)
            val upperRight = (mbr._3, mbr._4)

            //            if (QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(lowerLeft._1, upperRight._2), k, gridOp.getBoxW, gridOp.getBoxH).map(bvMapPartAssign.value.get(_).get).size >= 9)
            //                println(QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(lowerLeft._1, upperRight._2), k, gridOp.getBoxW, gridOp.getBoxH).map(bvMapPartAssign.value.get(_).get))

            List(QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(lowerLeft._1, lowerLeft._2), k).map(bvMapPartAssign.value.get(_).get).size,
                 //                 getPartitionsInRange(bvGlobalIndex.value, allQTMBR, (upperRight._1 - lowerLeft._1) / 2, lowerLeft._2,k).size,
                 QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(upperRight._1, lowerLeft._2), k).map(bvMapPartAssign.value.get(_).get).size,
                 //                 getPartitionsInRange(bvGlobalIndex.value, allQTMBR, upperRight._1, (upperRight._2 - lowerLeft._2) / 2,k,bvMapPartAssign.value).size,
                 QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(upperRight._1, upperRight._2), k).map(bvMapPartAssign.value.get(_).get).size,
                 //                 getPartitionsInRange(bvGlobalIndex.value, allQTMBR, (upperRight._1 - lowerLeft._1) / 2, upperRight._2,k).size,
                 QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(lowerLeft._1, upperRight._2), k).map(bvMapPartAssign.value.get(_).get).size).max
        }).max

        //        println("<>" + numRounds)

        (0 until numRounds).foreach(roundNumber => {

            rddPoint = rddQuadTree
                .mapPartitions(_.map(qtInf => {

                    val tuple: Any = qtInf

                    (bvMapPartAssign.value.get(qtInf.uniqueIdentifier).get, tuple)
                }) /*, true*/ )
                .union(rddPoint)
                .partitionBy(new Partitioner() {

                    override def numPartitions = actualPartitionCount
                    override def getPartition(key: Any): Int =
                        key match {
                            case partId: Int => partId
                        }
                })
                .mapPartitionsWithIndex((pIdx, iter) => {

                    val lstPartQT = ListBuffer[QuadTreeInfo]()

                    iter.map(row => {
                        row._2 match {

                            case qtInf: QuadTreeInfo => {

                                lstPartQT += qtInf

                                null
                            }
                            case _ => {

                                val (point, sortSetSqDist, lstUId) = row._2.asInstanceOf[(Point, SortSetObj, List[Int])]

                                //                                if (point.userData.toString().equalsIgnoreCase("taxi_b_601998"))
                                //                                    println(pIdx)

                                if (!lstUId.isEmpty) {

                                    // build a list of QT to check
                                    val lstVisitQTInf = lstPartQT.filter(qtInf => lstUId.contains(qtInf.uniqueIdentifier))

                                    //                                    if (point.userData.toString().equalsIgnoreCase("yellow_1_b_548388"))
                                    //                                        println(pIdx)

                                    QuadTreeOperations.nearestNeighbor(lstVisitQTInf, point, sortSetSqDist, k)

                                    val lstLeftQTInf = lstUId.filterNot(lstVisitQTInf.map(_.uniqueIdentifier).contains _)

                                    val ret = (if (lstLeftQTInf.isEmpty) row._1 else bvMapPartAssign.value.get(lstLeftQTInf.head).get, (point, sortSetSqDist, lstLeftQTInf))

                                    ret
                                }
                                else
                                    row
                            }
                        }
                    })
                        .filter(_ != null)
                })
        })

        rddPoint.mapPartitions(_.map(row => {

            val (point, sortSetSqDist, _) = row._2.asInstanceOf[(Point, SortSetObj, List[Int])]

            //            val point = row._2._1 match { case pt: Point => pt }
            //            val sortSetSqDist = row._2._2

            (point, sortSetSqDist.map(nd => (nd.distance, nd.data match { case pt: Point => pt })))
        }))
    }

    private def reducerListBuffer[T](lst1: ListBuffer[T], lst2: ListBuffer[T]): ListBuffer[T] =
        if (lst1 == null && lst2 == null) lst1
        else if (lst1 == null) lst2
        else if (lst2 == null) lst1
        else
            lst1 ++ lst2

    private def binarySearch(arrHorizDist: Array[(Double, Double, Long)], pointX: Long): Int = {

        var topIdx = 0
        var botIdx = arrHorizDist.length - 1

        while (botIdx >= topIdx) {

            val midIdx = (topIdx + botIdx) / 2
            val midRegion = arrHorizDist(midIdx)

            if (pointX >= midRegion._1 && pointX <= midRegion._2)
                return midIdx
            else if (pointX < midRegion._1)
                botIdx = midIdx - 1
            else
                topIdx = midIdx + 1
        }

        throw new Exception("binarySearch() for %,d failed in horizontal distribution %s".format(pointX, arrHorizDist.toString))
    }

    private def getDS1Stats(iter: Iterator[Point]) = {

        val (maxRowSize: Int, rowCount: Long, minX: Double, maxX: Double) = iter.fold(Int.MinValue, 0L, Double.MaxValue, Double.MinValue)((x, y) => {

            val (a, b, c, d) = x.asInstanceOf[(Int, Long, Double, Double)]
            val point = y match { case pt: Point => pt }

            (math.max(a, point.userData.toString.length), b + 1, math.min(c, point.x), math.max(d, point.x))
        })

        (maxRowSize, rowCount, minX, maxX)
    }

    private def computeCapacity(rddRight: RDD[Point], k: Int) = {

        // 7% reduction in memory to account for overhead operations
        var execAvailableMemory = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory", rddRight.context.getExecutorMemoryStatus.map(_._2._1).max + "B")) // skips memory of core assigned for Hadoop daemon
        // deduct yarn overhead
        val exeOverheadMemory = math.ceil(math.max(384, 0.1 * execAvailableMemory)).toLong

        val (maxRowSize, totalRowCount, minX, minY, maxX, maxY) = rddRight.mapPartitionsWithIndex((pIdx, iter) => {

            val (maxRowSize: Int, totalRowCount: Long, minX: Double, minY: Double, maxX: Double, maxY: Double) = iter.fold(Int.MinValue, 0L, Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)((tuple, point) => {

                val (maxRowSize, totalRowCount, minX, minY, maxX, maxY) = tuple.asInstanceOf[(Int, Long, Double, Double, Double, Double)]
                val pnt = point match { case pt: Point => pt }

                (math.max(maxRowSize, pnt.userData.toString.length), totalRowCount + 1, math.min(minX, pnt.x), math.min(minY, pnt.y), math.max(maxX, pnt.x), math.max(maxY, pnt.y))
            })

            Iterator((maxRowSize, totalRowCount, minX, minY, maxX, maxY))
        })
            .fold((0, 0L, Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))((t1, t2) => (math.max(t1._1, t2._1), t1._2 + t2._2, math.min(t1._3, t2._3), math.min(t1._4, t2._4), math.max(t1._5, t2._5), math.max(t1._6, t2._6)))

        //        val gmGeomDummy = GMPoint((0 until maxRowSize).map(_ => " ").mkString(""), (0, 0))
        val userData = Array.fill[Char](maxRowSize)(' ').toString
        val pointDummy = new Point(0, 0, userData)
        val quadTreeEmptyDummy = new QuadTreeInfo(Box(new Point(pointDummy), new Point(pointDummy)))
        val sortSetDummy = SortSetObj(k, false)

        val pointCost = SizeEstimator.estimate(pointDummy)
        val sortSetCost = /* pointCost + */ SizeEstimator.estimate(sortSetDummy) + (k * pointCost)
        val quadTreeCost = SizeEstimator.estimate(quadTreeEmptyDummy)

        // exec mem cost = 1QT + 1Pt and matches
        var execRowCapacity = (((execAvailableMemory - exeOverheadMemory - quadTreeCost) / pointCost) /*- (pointCost + sortSetCost)*/ ).toInt

        var numParts = math.ceil(totalRowCount.toDouble / execRowCapacity).toInt

        if (numParts == 1) {

            numParts = rddRight.getNumPartitions
            execRowCapacity = (totalRowCount / numParts).toInt
        }

        (execRowCapacity, totalRowCount, (minX, minY, maxX, maxY))
    }

    private def computeMBR(lstPoint: List[Point]) = {

        val (minX: Double, minY: Double, maxX: Double, maxY: Double) = lstPoint.fold(Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)((mbr, point) => {

            val (a, b, c, d) = mbr.asInstanceOf[(Double, Double, Double, Double)]
            val pt = point match { case pt: Point => pt }

            (math.min(a, pt.x), math.min(b, pt.y), math.max(c, pt.x), math.max(d, pt.y))
        })

        (minX, minY, maxX, maxY)
    }

    private def computePartitionRanges(rddRight: RDD[Point], execRowCapacity: Int) = {

        val arrRangesInfo = rddRight
            .mapPartitions(_.map(point => ((point.x / execRowCapacity).toLong, 1L)))
            .reduceByKey(_ + _)
            .collect
            .sortBy(_._1) // by bucket #
            .map(row => {

                val start = row._1 * execRowCapacity
                val end = start + execRowCapacity - 1

                (start, end, row._2)
            })

        val lstHorizRange = ListBuffer[(Double, Double, Long)]()
        var start = arrRangesInfo(0)._1
        var size = 0L
        var idx = 0

        while (idx < arrRangesInfo.size) {

            var row = arrRangesInfo(idx)

            while (size == 0 && row._3 >= execRowCapacity) {

                val end = row._1 + math.ceil((row._2 - row._1) * (execRowCapacity / row._3.toFloat)).toLong

                lstHorizRange.append((row._1, end, execRowCapacity))

                start = end + 1
                row = (start, row._2, row._3 - execRowCapacity)
            }

            val needCount = execRowCapacity - size
            val availCount = if (needCount > row._3) row._3 else needCount

            size += availCount

            if (size == execRowCapacity || idx == arrRangesInfo.size - 1) {

                if (availCount == row._3) {

                    lstHorizRange.append((start, row._2, size))
                    idx += 1
                    if (idx < arrRangesInfo.size) start = arrRangesInfo(idx)._1
                }
                else {

                    // val end = start + execRowCapacity - 1
                    val end = row._1 + math.ceil((row._2 - row._1) * (availCount / row._3.toFloat)).toLong

                    lstHorizRange.append((start, end, size))
                    start = end + 1

                    arrRangesInfo(idx) = (start, row._2, row._3 - availCount)
                }

                size = 0
            }
            else
                idx += 1
        }

        lstHorizRange.toArray
    }
}