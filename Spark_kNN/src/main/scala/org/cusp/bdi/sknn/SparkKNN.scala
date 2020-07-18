package org.cusp.bdi.sknn

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
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

import scala.collection.immutable

case class PartitionInfo(left: (Double, Double), bottom: (Double, Double), right: (Double, Double), top: (Double, Double), uniqueIdentifier: Int, totalPoints: Long) {

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

  private def knnJoin(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int): RDD[(Point, Iterable[(Double, Point)])] /*: RDD[(Point, Iterable[(Double, Point)])]*/ = {

    var (execRowCapacity, totalRowCount) = computeCapacity(rddRight, k)

    //        execRowCapacity = 57702

    //        println(">>" + execRowCapacity)

    var arrPartRangeCount = computePartitionRanges(rddRight, execRowCapacity)

    var arrPartInf = rddRight.mapPartitions(_.map(point => (point.x, point.y)))
      .repartitionAndSortWithinPartitions(new Partitioner() {

        // places in containers along the x-axis and sort by x-coor
        override def numPartitions = arrPartRangeCount.size

        override def getPartition(key: Any): Int =
          key match {
            case xCoord: Double => binarySearchArr(arrPartRangeCount, xCoord.toLong)
          }
      })
      .mapPartitionsWithIndex((pIdx, iter) => {

        val lstPartitionRangeCount = ListBuffer[PartitionInfo]()

        var left = iter.next
        var bottom = left
        var right = left
        var top = left

        var count = 1

        while (iter.hasNext) {

          var xy = iter.next
          count += 1

          right = xy

          if (xy._2 < bottom._2) bottom = xy
          else if (xy._2 > top._2) top = xy

          if (count == execRowCapacity || !iter.hasNext) {

            lstPartitionRangeCount.append(PartitionInfo(left, bottom, right, top, Random.nextInt(), count))

            if (iter.hasNext) {

              left = iter.next
              bottom = left
              right = left
              top = left
              count = 1
            }
          }
        }

        lstPartitionRangeCount.iterator
      })
      .collect
      .sortBy(_.left._1)

    arrPartRangeCount = null

    val actualPartitionCount = AssignToPartitions(arrPartInf, execRowCapacity).getPartitionCount

    //    arrPartInf.foreach(pInf => println(">1>\t%s\t%s\t%s\t%s\t%d\t%d\t%d".format(pInf.left._1, pInf.bottom._2, pInf.right._1, pInf.top._2, pInf.totalPoints, pInf.assignedPart, pInf.uniqueIdentifier)))

    val rddSpIdx = rddRight
      .mapPartitions(_.map(point => (point, 0.toByte)))
      .partitionBy(new Partitioner() {
        override def numPartitions: Int = actualPartitionCount

        override def getPartition(key: Any): Int = key match {
          case point: Point => binarySearchPartInf(arrPartInf, point.x).assignedPart
        }
      })
      .mapPartitionsWithIndex((pIdx, iter) => {

        val mapSpIdx = HashMap[Int, QuadTreeInfo]()
        var qtInf: QuadTreeInfo = null

        iter.foreach(row => {

          val partInf = binarySearchPartInf(arrPartInf, row._1.x)

          val spIdx = mapSpIdx.getOrElse(partInf.uniqueIdentifier, {

            val minX = partInf.left._1.toLong
            val minY = partInf.bottom._2.toLong
            val maxX = partInf.right._1.toLong + 1
            val maxY = partInf.top._2.toLong + 1

            val halfWidth = (maxX - minX) / 2.0
            val halfHeight = (maxY - minY) / 2.0

            val newIdx = new QuadTreeInfo(Box(new Point(halfWidth + minX, halfHeight + minY), new Point(halfWidth, halfHeight)))
            newIdx.uniqueIdentifier = partInf.uniqueIdentifier

            mapSpIdx += (partInf.uniqueIdentifier -> newIdx)

            newIdx
          })

          spIdx.quadTree.insert(row._1)
        })

        mapSpIdx.valuesIterator
      }, true)
      .persist(StorageLevel.MEMORY_ONLY)

    //    rddSpIdx.foreach(qtInf => println(">2>\t%s".format(qtInf.toString())))

    val mbrDS1 = arrPartInf.map(partInf => (partInf.left._1, partInf.bottom._2, partInf.right._1, partInf.top._2))
      .fold((Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))((mbr1, mbr2) => (math.min(mbr1._1, mbr2._1), math.min(mbr1._2, mbr2._2), math.max(mbr1._3, mbr2._3), math.max(mbr1._4, mbr2._4)))

    // (box#, Count)
    var arrGridAndSpIdxInf = rddSpIdx
      .mapPartitionsWithIndex((pIdx, iter) => {


        val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

        iter.map(qtInf =>
          qtInf.quadTree.getAllPoints.iterator.map(point =>
            (gridOp.computeBoxXY(point.x, point.y), (1L, Set(qtInf.uniqueIdentifier)))
          )
        )
          .flatMap(_.seq)
      })
      .reduceByKey((x, y) => (x._1 + y._1, x._2 ++ y._2))
      .collect

    val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

    var leftBot = gridOp.computeBoxXY(mbrDS1._1, mbrDS1._2)
    var rightTop = gridOp.computeBoxXY(mbrDS1._3, mbrDS1._4)

    val halfWidth = ((rightTop._1 - leftBot._1).toLong + 1) / 2.0
    val halfHeight = ((rightTop._2 - leftBot._2).toLong + 1) / 2.0

    val globalIndex = new QuadTreeDigest(Box(new Point(halfWidth + leftBot._1, halfHeight + leftBot._2), new Point(halfWidth, halfHeight)))

    arrGridAndSpIdxInf.foreach(row => globalIndex.insert((row._1._1, row._1._2), row._2._1, row._2._2))

    val bvQTGlobalIndex = rddLeft.context.broadcast(globalIndex)

    val bvMapUIdPartId = rddLeft.context.broadcast(arrPartInf.map(partInf => (partInf.uniqueIdentifier, partInf.assignedPart)).toMap)

    var rddPoint = rddLeft
      .mapPartitions(iter => {

        val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

        iter.map(point => {

          //                    if (point.userData.toString().equalsIgnoreCase("yellow_1_b_548388"))
          //                        println

          val lstUId = QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k)
            .toList

          //println(">>\t"+lstUId.size)

          //            if (lstUId.size >= 11)
          //              println(QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k))

          val tuple: Any = (point, SortSetObj(k, false), lstUId)

          (bvMapUIdPartId.value.get(lstUId.head).get, tuple)
        })
      })

    //        println("<>" + rddPoint.mapPartitions(_.map(_._2.asInstanceOf[(Point, SortSetObj, List[Int])]._3.size)).max)
    //        println(QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(1013487.21, 134367.52), k))

    val numRounds = arrPartInf
      .map(partInf => {

        //        if (QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.bottom), k).map(bvMapUIdPartId.value.get(_).get).size >= 11)
        //          println(QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.bottom), k).map(bvMapUIdPartId.value.get(_).get))

        List(QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.left), k).map(bvMapUIdPartId.value.get(_).get).size,
          //                 getPartitionsInRange(bvGlobalIndex.value, allQTMBR, (upperRight._1 - lowerLeft._1) / 2, lowerLeft._2,k).size,
          QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.bottom), k).map(bvMapUIdPartId.value.get(_).get).size,
          //                 getPartitionsInRange(bvGlobalIndex.value, allQTMBR, upperRight._1, (upperRight._2 - lowerLeft._2) / 2,k,bvMapUIdPartId.value).size,
          QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.right), k).map(bvMapUIdPartId.value.get(_).get).size,
          //                 getPartitionsInRange(bvGlobalIndex.value, allQTMBR, (upperRight._1 - lowerLeft._1) / 2, upperRight._2,k).size,
          QuadTreeDigestOperations.getPartitionsInRange(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.top), k).map(bvMapUIdPartId.value.get(_).get).size).max
      }).max

    arrPartInf = null

    //        println("<>" + numRounds)

    (0 until numRounds).foreach(roundNumber => {

      rddPoint = rddSpIdx
        .mapPartitions(_.map(qtInf => {

          val tuple: Any = qtInf

          (bvMapUIdPartId.value.get(qtInf.uniqueIdentifier).get, tuple)
        }) /*, true*/)
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

                  //  if (point.userData.toString().equalsIgnoreCase("yellow_1_b_548388"))
                  //    println(pIdx)

                  QuadTreeOperations.nearestNeighbor(lstVisitQTInf, point, sortSetSqDist, k)

                  val lstLeftQTInf = lstUId.filterNot(lstVisitQTInf.map(_.uniqueIdentifier).contains _)

                  val ret = (if (lstLeftQTInf.isEmpty) row._1 else bvMapUIdPartId.value.get(lstLeftQTInf.head).get, (point, sortSetSqDist, lstLeftQTInf))

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

      (point, sortSetSqDist.map(nd => (nd.distance, nd.data match {
        case pt: Point => pt
      })))
    }))
  }

  private def binarySearchArr(arrHorizDist: Array[(Double, Double)], pointX: Long): Int = {

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

    throw new Exception("binarySearchArr() for %,d failed in horizontal distribution %s".format(pointX, arrHorizDist.toString))
  }

  private def binarySearchPartInf(arrPartInf: Array[PartitionInfo], pointX: Double): PartitionInfo = {

    var topIdx = 0
    var botIdx = arrPartInf.length - 1

    while (botIdx >= topIdx) {

      val midIdx = (topIdx + botIdx) / 2
      val midRegion = arrPartInf(midIdx)

      if (pointX >= midRegion.left._1 && pointX <= midRegion.right._1)
        return midRegion
      else if (pointX < midRegion.left._1)
        botIdx = midIdx - 1
      else
        topIdx = midIdx + 1
    }

    throw new Exception("binarySearchPartInf() for %,d failed in horizontal distribution %s".format(pointX, arrPartInf.toString))
  }

  //  private def getDS1Stats(iter: Iterator[Point]) = {
  //
  //    val (maxRowSize: Int, rowCount: Long, minX: Double, maxX: Double) = iter.fold(Int.MinValue, 0L, Double.MaxValue, Double.MinValue)((x, y) => {
  //
  //      val (a, b, c, d) = x.asInstanceOf[(Int, Long, Double, Double)]
  //      val point = y match {
  //        case pt: Point => pt
  //      }
  //
  //      (math.max(a, point.userData.toString.length), b + 1, math.min(c, point.x), math.max(d, point.x))
  //    })
  //
  //    (maxRowSize, rowCount, minX, maxX)
  //  }

  private def computeCapacity(rddRight: RDD[Point], k: Int) = {

    // 7% reduction in memory to account for overhead operations
    var execAvailableMemory = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory", rddRight.context.getExecutorMemoryStatus.map(_._2._1).max + "B")) // skips memory of core assigned for Hadoop daemon
    // deduct yarn overhead
    val exeOverheadMemory = math.ceil(math.max(384, 0.1 * execAvailableMemory)).toLong

    val (maxRowSize, totalRowCount) = rddRight.mapPartitionsWithIndex((pIdx, iter) => {

      val (maxRowSize: Int, totalRowCount: Long) = iter.fold(0, 0L)((tuple, point) => {

        val (maxRowSize, totalRowCount) = tuple.asInstanceOf[(Int, Long)]
        val pnt = point match {
          case pt: Point => pt
        }

        (math.max(maxRowSize, pnt.userData.toString.length), totalRowCount + 1)
      })

      Iterator((maxRowSize, totalRowCount))
    })
      .fold((0, 0L))((t1, t2) => (math.max(t1._1, t2._1), t1._2 + t2._2))

    //        val gmGeomDummy = GMPoint((0 until maxRowSize).map(_ => " ").mkString(""), (0, 0))
    val userData = Array.fill[Char](maxRowSize)(' ').toString
    val pointDummy = new Point(0, 0, userData)
    val quadTreeEmptyDummy = new QuadTreeInfo(Box(new Point(pointDummy), new Point(pointDummy)))
    val sortSetDummy = SortSetObj(k, false)

    val pointCost = SizeEstimator.estimate(pointDummy)
    val sortSetCost = /* pointCost + */ SizeEstimator.estimate(sortSetDummy) + (k * pointCost)
    val quadTreeCost = SizeEstimator.estimate(quadTreeEmptyDummy)

    // exec mem cost = 1QT + 1Pt and matches
    var execRowCapacity = (((execAvailableMemory - exeOverheadMemory - quadTreeCost) / pointCost)).toInt

    var numParts = math.ceil(totalRowCount.toDouble / execRowCapacity).toInt

    if (numParts == 1) {

      numParts = rddRight.getNumPartitions
      execRowCapacity = (totalRowCount / numParts).toInt
    }

    (execRowCapacity, totalRowCount)
  }

  private def computeMBR(lstPoint: List[Point]) = {

    val (minX: Double, minY: Double, maxX: Double, maxY: Double) = lstPoint.fold(Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)((mbr, point) => {

      val (a, b, c, d) = mbr.asInstanceOf[(Double, Double, Double, Double)]
      val pt = point match {
        case pt: Point => pt
      }

      (math.min(a, pt.x), math.min(b, pt.y), math.max(c, pt.x), math.max(d, pt.y))
    })

    (minX, minY, maxX, maxY)
  }

  private def computePartitionRanges(rddRight: RDD[Point], execRowCapacity: Int) = {

    val arrContainerRangeAndCount = rddRight
      .mapPartitions(_.map(point => ((point.x / execRowCapacity).toLong, 1L)))
      .reduceByKey(_ + _)
      .collect
      .sortBy(_._1) // by bucket #
      .map(row => {

        val start = row._1 * execRowCapacity
        val end = start + execRowCapacity - 1

        (start, end, row._2)
      })

    val lstPartRangeCount = ListBuffer[(Double, Double, Long)]()
    val lastIdx = arrContainerRangeAndCount.size - 1
    var idx = 0
    var row = arrContainerRangeAndCount(idx)
    var start = row._1
    var totalFound = 0L

    //        arrContainerRangeAndCount.foreach(row => println(">3>\t%d\t%d\t%d".format(row._1, row._2, row._3)))

    while (idx <= lastIdx) {

      val borrowCount = execRowCapacity - totalFound

      if (borrowCount > row._3 && idx != lastIdx) {

        totalFound += row._3
        idx += 1
        row = arrContainerRangeAndCount(idx)
      }
      else {

        val percent = if (borrowCount > row._3.toDouble) 1 else borrowCount / row._3.toDouble

        val end = row._1 + math.ceil((row._2 - row._1) * percent).toLong

        lstPartRangeCount.append((start, end, execRowCapacity))

        if (idx == lastIdx)
          idx += 1
        else {

          totalFound = 0

          if (borrowCount == row._3) {

            idx += 1
            row = arrContainerRangeAndCount(idx)
            start = row._1
          }
          else {

            start = end + 1
            row = (start, row._2, row._3 - borrowCount)
          }
        }
      }
    }

    //    lstPartRangeCount.foreach(row => println(">4>\t%.8f\t%.8f\t%d".format(row._1, row._2, row._3)))

    lstPartRangeCount.map(row => (row._1, row._2)).toArray
  }
}