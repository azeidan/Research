package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.qt.{QuadTree, QuadTreeOperations}
import org.cusp.bdi.ds.{Box, Point}
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.{Helper, Node, SortedList}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

class RangeInfo() extends Serializable {

  var totalPoints = 0L
  var assignedPart = -1
  var left = 0.0
  var bottom = 0.0
  var right = 0.0
  var top = 0.0

  def this(_left: Double, _bottom: Double, _right: Double, _top: Double) {

    this()
    this.left = _left
    this.bottom = _bottom
    this.right = _right
    this.top = _top
  }

  def this(other: RangeInfo) =
    this(other.left, other.bottom, other.right, other.top)

  override def toString: String =
    "%.8f\t%.8f\t%.8f\t%.8f\t%d".format(left, bottom, right, top, totalPoints)
}

case class PartitionInfo() extends RangeInfo {

  var uniqueIdentifier: Int = -1

  def this(_uniqueIdentifier: Int, _left: Double, _bottom: Double, _right: Double, _top: Double, _totalPoints: Long) {

    this()
    this.uniqueIdentifier = _uniqueIdentifier
    this.left = _left
    this.bottom = _bottom
    this.right = _right
    this.top = _top
    this.totalPoints = _totalPoints
  }

  override def toString: String =
    "%.8f\t%.8f\t%.8f\t%.8f\t%d\t%d\t%d".format(left, bottom, right, top, assignedPart, uniqueIdentifier, totalPoints)
}

class RowData(_point: Point, _sortedList: SortedList[Point], _lstQuadTreeUId: List[Int]) {

  val point = _point
  val sortedList = _sortedList
  var lstQuadTreeUId = _lstQuadTreeUId
}

case class GlobalIndexPointData(numPoints: Long, setUId: Set[Int]) extends Serializable with Comparable[GlobalIndexPointData] {
  override def compareTo(other: GlobalIndexPointData) = 1
}

object SparkKNN {

  def getSparkKNNClasses: Array[Class[_]] =
    Array(classOf[RangeInfo],
      classOf[PartitionInfo],
      classOf[RowData],
      classOf[GlobalIndexPointData],
      classOf[QuadTree],
      QuadTreeOperations.getClass,
      classOf[GridOperation],
      classOf[SpatialIndexInfo],
      Helper.getClass,
      classOf[Node[_]],
      classOf[SortedList[_]],
      classOf[Box],
      classOf[Point],
      classOf[AssignToPartitions],
      RDD_Store.getClass,
      SparkKNN_Arguments.getClass)
}

case class SparkKNN(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int) {

  // for testing, remove ...
  var minPartitions = 0

  def knnJoin() =
    knnJoinExecute(rddLeft, rddRight)

  def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] =
    knnJoinExecute(rddLeft, rddRight)
      .union(knnJoinExecute(rddRight, rddLeft))

  private def knnJoinExecute(rddLeft: RDD[Point], rddRight: RDD[Point]): RDD[(Point, Iterable[(Double, Point)])] = {

    val (execRowCapacity, totalRowCount, mbrDS1) = computeCapacity(rddRight)

    //        execRowCapacity = 57702
    //        println(">>" + execRowCapacity)

    var arrRangeInf = computeRangeInfo(rddRight, mbrDS1, execRowCapacity, totalRowCount)

    //    arrRangeInf.foreach(rInf => println(">1>\t%.8f\t%.8f\t%.8f\t%.8f\t%d\t%d".format(rInf.left, rInf.bottom, rInf.right, rInf.top, rInf.totalPoints, rInf.assignedPart)))

    implicit def ordering[A <: Point]: Ordering[A] = new Ordering[A] {
      override def compare(point1: A, point2: A): Int =
        point1.x.compareTo(point2.x)
    }

    val rddSpIdx = rddRight
      .mapPartitions(_.map(point => (point, Byte.MinValue)))
      .repartitionAndSortWithinPartitions(new Partitioner() {
        override def numPartitions: Int = arrRangeInf.length

        override def getPartition(key: Any): Int = key match {
          case point: Point => binarySearchPartInf(arrRangeInf, point.x).assignedPart //mapUIdPartId(pId).assignedPart
        }
      })
      .mapPartitions(iter => {

        val mapSpIdx = mutable.HashMap[Int, SpatialIndexInfo]()
        var point: Point = null
        var lstPoints = ListBuffer[Point]()
        var buildIndex = false

        var pointLast: Point = null
        var minX = Double.MaxValue
        var minY = Double.MaxValue
        var maxX = Double.MinValue
        var maxY = Double.MinValue

        while (iter.hasNext || point != null) {

          if (point == null)
            point = iter.next._1

          if (lstPoints.isEmpty) {

            minX = point.x
            minY = point.y
            maxX = minX
            maxY = minY
          }

          if (pointLast == null || point.x <= pointLast.x) {

            lstPoints.append(point)

            if (point.x > maxX) maxX = point.x

            if (point.y < minY) minY = point.y
            else if (point.y > maxY) maxY = point.y

            if (pointLast == null && lstPoints.length == execRowCapacity)
              pointLast = point

            point = null
          }
          else
            buildIndex = true

          if (buildIndex || !iter.hasNext) {

            minX = minX.toLong
            maxX = maxX.toLong + 1
            minY = minY.toLong
            maxY = maxY.toLong + 1

            val halfWidth = (maxX - minX) / 2
            val halfHeight = (maxY - minY) / 2

            val sIdxInfo = new SpatialIndexInfo(Box(new Point(halfWidth + minX, halfHeight + minY), new Point(halfWidth, halfHeight)))
            sIdxInfo.uniqueIdentifier = Random.nextInt
            lstPoints.foreach(sIdxInfo.quadTree.insert)
            mapSpIdx += (sIdxInfo.uniqueIdentifier -> sIdxInfo)

            // reset
            lstPoints = ListBuffer[Point]()
            pointLast = null
            buildIndex = false
            minX = Double.MaxValue
            minY = Double.MaxValue
            maxX = Double.MinValue
            maxY = Double.MinValue
          }
        }

        mapSpIdx.valuesIterator
      } /*, preservesPartitioning = true*/)
      .persist(StorageLevel.MEMORY_ONLY)

    val arrPartInf = rddSpIdx.mapPartitions(_.map(qtInf => new PartitionInfo(qtInf.uniqueIdentifier, qtInf.quadTree.boundary.left, qtInf.quadTree.boundary.bottom, qtInf.quadTree.boundary.right, qtInf.quadTree.boundary.top, qtInf.quadTree.getTotalPoints)))
      .collect

    val actualPartitionCount = AssignToPartitions(arrPartInf, execRowCapacity).getPartitionCount

    val mapUIdPartId = arrPartInf.map(partInf => (partInf.uniqueIdentifier, partInf)).toMap

    //    arrPartInf.foreach(pInf => println(">1>\t%.8f\t%.8f\t%.8f\t%.8f\t%d\t%d\t%d".format(pInf.left, pInf.bottom, pInf.right, pInf.top, pInf.totalPoints, pInf.assignedPart, pInf.uniqueIdentifier)))
    //
    //    println(">2>=====================================")
    //    rddSpIdx.foreach(qtInf => println(">2>\t%d%s%n".format(mapUIdPartId(qtInf.uniqueIdentifier).assignedPart, qtInf.toString())))
    //    println(">2>=====================================")

    //    var mbrDS1 = arrRangeInf.map(partInf => (partInf.left, partInf.bottom, partInf.right, partInf.top))
    //      .fold((Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))((mbr1, mbr2) => (math.min(mbr1._1, mbr2._1), math.min(mbr1._2, mbr2._2), math.max(mbr1._3, mbr2._3), math.max(mbr1._4, mbr2._4)))

    val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

    val leftBot = gridOp.computeBoxXY(mbrDS1._1, mbrDS1._2)
    val rightTop = gridOp.computeBoxXY(mbrDS1._3, mbrDS1._4)

    //    mbrDS1 = null

    val halfWidth = ((rightTop._1 - leftBot._1) + 1) / 2.0
    val halfHeight = ((rightTop._2 - leftBot._2) + 1) / 2.0

    val globalIndex = new QuadTree(Box(new Point(halfWidth + leftBot._1, halfHeight + leftBot._2), new Point(halfWidth, halfHeight)))

    val bvQTGlobalIndex = rddLeft.context.broadcast(globalIndex)

    // (box#, Count)
    rddSpIdx
      .mapPartitions(_.map(qtInf => qtInf.quadTree
        .getAllPoints
        .iterator
        .map(_.map(point => (gridOp.computeBoxXY(point.x, point.y), (1L, Set(qtInf.uniqueIdentifier))))))
        .flatMap(_.seq)
        .flatMap(_.seq)
      )
      .reduceByKey((x, y) => (x._1 + y._1, x._2 ++ y._2))
      .collect
      .foreach(row => globalIndex.insert(new Point(row._1._1, row._1._2, GlobalIndexPointData(row._2._1, row._2._2))))

    //    arrGridAndSpIdxInf.foreach(row => println(">3>\t%d\t%d\t%d\t%s".format(row._1._1, row._1._2, row._2._1, row._2._2.mkString(","))))

    arrRangeInf = null

    var rddPoint = rddLeft
      .mapPartitions(iter => {

        iter.map(point => {

          //          if (point.userData.toString().equalsIgnoreCase("Yellow_3_B_925635"))
          //            println

          val lstUId = QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k /*, gridOp.getErrorRange*/)
            .toList

          val rowPointData: Any = new RowData(point, SortedList[Point](k, allowDuplicates = false), lstUId)

          (mapUIdPartId(lstUId.head).assignedPart, rowPointData)
        })
      })

    val numRounds = rddLeft
      .mapPartitions(iter => {

        val b = 0.toByte

        iter.map(point => (gridOp.computeBoxXY(point.x, point.y), b))
      })
      .reduceByKey((x, _) => x)
      .mapPartitions(iter => {

        Iterator(iter.map(row => {

          //          if (QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, row._1, k, gridOp.getErrorRange).map(mapUIdPartId(_).assignedPart).size >= 8)
          //            println(QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, row._1, k, gridOp.getErrorRange).map(mapUIdPartId(_).assignedPart).size)

          QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, row._1, k /*, gridOp.getErrorRange*/).map(mapUIdPartId(_).assignedPart).size
        }).max)
      })
      .max

    (0 until numRounds).foreach(_ => {

      rddPoint = rddSpIdx
        .mapPartitions(_.map(qtInf => {

          val tuple: Any = qtInf

          (mapUIdPartId(qtInf.uniqueIdentifier).assignedPart, tuple)
        }) /*, true*/)
        .union(rddPoint)
        .partitionBy(new Partitioner() {

          override def numPartitions: Int = actualPartitionCount

          override def getPartition(key: Any): Int =
            key match {
              case partId: Int => partId
            }
        })
        .mapPartitions(iter => {

          val lstPartQT = ListBuffer[SpatialIndexInfo]()

          iter.map(row => {

            row._2 match {

              case qtInf: SpatialIndexInfo =>

                lstPartQT += qtInf

                null
              case rowPointData: RowData =>

                if (rowPointData.lstQuadTreeUId == null)
                  row
                else {

                  //                  if (rowPointData.point.userData.toString().equalsIgnoreCase("yellow_3_a_772558"))
                  //                    println

                  // build a list of QT to check
                  val lstVisitQTInf = lstPartQT.filter(partQT => rowPointData.lstQuadTreeUId.contains(partQT.uniqueIdentifier))

                  QuadTreeOperations.nearestNeighbor(lstVisitQTInf, rowPointData.point, rowPointData.sortedList, k)

                  // randomize order to lessen query skews
                  val lstLeftQTInf = Random.shuffle(rowPointData.lstQuadTreeUId.filterNot(lstVisitQTInf.map(_.uniqueIdentifier).contains _))

                  rowPointData.lstQuadTreeUId = if (lstLeftQTInf.isEmpty) null else lstLeftQTInf

                  (if (lstLeftQTInf.isEmpty) row._1 else mapUIdPartId(lstLeftQTInf.head).assignedPart, rowPointData)

                  //                  if (lstLeftQTInf.isEmpty)
                  //                    (row._1, (point, sortSetSqDist, null))
                  //                  else
                  //                    (mapUIdPartId(lstLeftQTInf.head).assignedPart, (point, sortSetSqDist, lstLeftQTInf))
                }
            }
          })
            .filter(_ != null)
        })
    })

    rddPoint.mapPartitions(_.map(_._2 match { case rowPointData: RowData => rowPointData })
      .map(rowPointData => (rowPointData.point, rowPointData.sortedList.map(nd => (nd.distance, nd.data)))))
  }

  //  private def binarySearchArrPartRange(arrPartRange: => Array[(Double, Double)], pointX: => Long): Int = {
  //
  //    var topIdx = 0
  //    var botIdx = arrPartRange.length - 1
  //
  //    while (botIdx >= topIdx) {
  //
  //      val midIdx = (topIdx + botIdx) / 2
  //      val midRegion = arrPartRange(midIdx)
  //
  //      if (pointX >= midRegion._1 && pointX <= midRegion._2)
  //        return midIdx
  //      else if (pointX < midRegion._1)
  //        botIdx = midIdx - 1
  //      else
  //        topIdx = midIdx + 1
  //    }
  //
  //    throw new Exception("binarySearchArrPartRange() for %,d failed in horizontal distribution %s".format(pointX, arrPartRange.mkString("Array(", ", ", ")")))
  //  }

  private def binarySearchPartInf(arrPartInf: Array[RangeInfo], pointX: Double): RangeInfo = {

    var topIdx = 0
    var botIdx = arrPartInf.length - 1

    while (botIdx >= topIdx) {

      val midIdx = (topIdx + botIdx) / 2
      val midRegion = arrPartInf(midIdx)

      if (pointX >= midRegion.left && pointX <= midRegion.right)
        return midRegion
      else if (pointX < midRegion.left)
        botIdx = midIdx - 1
      else
        topIdx = midIdx + 1
    }

    throw new Exception("binarySearchPartInf() for %.8f failed in horizontal distribution %s".format(pointX, arrPartInf.mkString("Array(", ", ", ")")))
  }

  private def computeCapacity(rddRight: RDD[Point]) = {

    // 7% reduction in memory to account for overhead operations
    val execAvailableMemory = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory", rddRight.context.getExecutorMemoryStatus.map(_._2._1).max + "B")) // skips memory of core assigned for Hadoop daemon
    // deduct yarn overhead
    val exeOverheadMemory = math.max(384, 0.1 * execAvailableMemory).toLong + 1

    val (maxRowSize, totalRowCount, mbrDS1Left, mbrDS1Bottom, mbrDS1Right, mbrDS1Top) = rddRight.mapPartitions(iter =>
      Iterator(iter.map(point => (point.userData.toString.length, 1L, point.x, point.y, point.x, point.y))
        .fold(0, 0L, Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)((param1, param2) =>
          (math.max(param1._1, param2._1), param1._2 + param2._2, math.min(param1._3, param2._3), math.min(param1._4, param2._4), math.max(param1._5, param2._5), math.max(param1._6, param2._6)))
      ))
      .fold(0, 0L, Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)((param1, param2) =>
        (math.max(param1._1, param2._1), param1._2 + param2._2, math.min(param1._3, param2._3), math.min(param1._4, param2._4), math.max(param1._5, param2._5), math.max(param1._6, param2._6)))

    //        val gmGeomDummy = GMPoint((0 until maxRowSize).map(_ => " ").mkString(""), (0, 0))
    val userData = Array.fill[Char](maxRowSize)(' ').mkString("Array(", ", ", ")")
    val pointDummy = new Point(0, 0, userData)
    val quadTreeEmptyDummy = new SpatialIndexInfo(Box(new Point(pointDummy), new Point(pointDummy)))
    val sortSetDummy = SortedList[String](k, allowDuplicates = false)

    val pointCost = SizeEstimator.estimate(pointDummy)
    val sortSetCost = /* pointCost + */ SizeEstimator.estimate(sortSetDummy) + (k * pointCost)
    val quadTreeCost = SizeEstimator.estimate(quadTreeEmptyDummy)

    // exec mem cost = 1QT + 1Pt and matches
    var execRowCapacity = ((execAvailableMemory - exeOverheadMemory - quadTreeCost) / pointCost).toInt

    var numParts = (totalRowCount.toDouble / execRowCapacity).toInt + 1

    if (numParts == 1) {

      numParts = rddRight.getNumPartitions
      execRowCapacity = (totalRowCount / numParts).toInt
    }

    (execRowCapacity, totalRowCount, (mbrDS1Left, mbrDS1Bottom, mbrDS1Right, mbrDS1Top))
  }

  private def computeRangeInfo(rddRight: RDD[Point], mbrDS1: (Double, Double, Double, Double), execRowCapacity: Int, totalRowCount: Long) = {

    val lstRangeInfo = rddRight
      .mapPartitions(_.map(_.xy))
      .repartitionAndSortWithinPartitions(new Partitioner() {

        // places in containers along the x-axis and sort by x-coords
        override def numPartitions: Int = math.ceil((mbrDS1._3 - mbrDS1._1) / execRowCapacity).toInt + 1

        override def getPartition(key: Any): Int =
          key match {
            case xCoord: Double =>
              ((xCoord - mbrDS1._1) / execRowCapacity).toInt
          }
      })
      .mapPartitions(iter => {

        val lstRangeInfo = ListBuffer[RangeInfo]()
        var rangeInf: RangeInfo = null

        while (iter.hasNext) {

          val row = iter.next

          if (rangeInf == null || rangeInf.totalPoints == execRowCapacity) {

            rangeInf = new RangeInfo(row._1, row._2, row._1, row._2)
            rangeInf.totalPoints += 1
            lstRangeInfo.append(rangeInf)
          }
          else {

            rangeInf.totalPoints += 1

            rangeInf.right = row._1

            if (row._2 < rangeInf.bottom) rangeInf.bottom = row._2
            else if (row._2 > rangeInf.top) rangeInf.top = row._2
          }
        }

        lstRangeInfo.iterator
      })
      .collect
      .sortBy(_.left) // can it be eliminated???
      .to[ListBuffer]

    var idx = -1
    var partCounter = 0

    lstRangeInfo.map(currRangeInfo => {

      idx += 1

      if (currRangeInfo.totalPoints == execRowCapacity) {

        currRangeInfo.assignedPart = partCounter
        partCounter += 1

        currRangeInfo
      }
      else if (currRangeInfo.totalPoints < execRowCapacity) {
        // merge or nothing if it's the last entry
        if (idx + 1 < lstRangeInfo.size) {

          val nexRangeInfo = lstRangeInfo(idx + 1)

          nexRangeInfo.left = currRangeInfo.left
          nexRangeInfo.totalPoints += currRangeInfo.totalPoints
          nexRangeInfo.bottom = math.min(currRangeInfo.bottom, nexRangeInfo.bottom)
          nexRangeInfo.top = math.max(currRangeInfo.top, nexRangeInfo.top)

          null
        }
        else {

          currRangeInfo.assignedPart = partCounter
          partCounter += 1

          currRangeInfo
        }
      }
      else /*if (currRangeInfo.totalPoints > execRowCapacity)*/ {

        val percent = execRowCapacity / currRangeInfo.totalPoints.toDouble

        // shrink current range
        val currRight = currRangeInfo.right
        val currTotalPoints = currRangeInfo.totalPoints
        currRangeInfo.assignedPart = partCounter
        partCounter += 1

        currRangeInfo.right = currRangeInfo.left + (currRangeInfo.right - currRangeInfo.left) * percent
        currRangeInfo.totalPoints = execRowCapacity

        // adjust or create next range
        var nexRangeInfo = if (idx + 1 < lstRangeInfo.length)
          lstRangeInfo(idx + 1)
        else {

          val rInf = new RangeInfo(currRangeInfo.left, currRangeInfo.bottom, currRight, currRangeInfo.top)
          lstRangeInfo.append(rInf)
          rInf
        }

        nexRangeInfo.left = currRangeInfo.right
        nexRangeInfo.totalPoints += currTotalPoints - execRowCapacity
        nexRangeInfo.bottom = math.min(currRangeInfo.bottom, nexRangeInfo.bottom)
        nexRangeInfo.top = math.max(currRangeInfo.top, nexRangeInfo.top)

        currRangeInfo
      }
    })
      .filter(_ != null)
      .toArray
  }
}