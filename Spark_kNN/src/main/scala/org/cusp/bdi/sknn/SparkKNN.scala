package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.qt.{QuadTree, QuadTreeOperations}
import org.cusp.bdi.ds.{Box, Point, PointBase}
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.{Helper, Node, SortedList}

import scala.collection.mutable.ListBuffer
import scala.util.Random

case class RangeInfo(assignedPartition: Int, startObj: ((Double, Double), Long)) extends Serializable {

  val lstBoxCoords = ListBuffer[((Double, Double), Long)]()
  var totalWeight = 0L
  val leftObj = startObj
  var bottomObj = startObj
  var rightObj = startObj
  var topObj = startObj

  def mbr = (leftObj._1._1, bottomObj._1._2, rightObj._1._1, topObj._1._2)

  override def toString: String =
    "%d\t%f\t%f\t%f\t%f\t%d".format(assignedPartition, leftObj._1._1, bottomObj._1._2, rightObj._1._1, topObj._1._2, totalWeight)
}

class RowData(_point: Point, _sortedList: SortedList[Point], _lstPartitionId: List[Int]) {

  val point = _point
  val sortedList = _sortedList
  var lstPartitionId = _lstPartitionId
}

case class GlobalIndexPointData(numPoints: Long, partitionIdx: Int) extends Serializable {}

object SparkKNN {

  def getSparkKNNClasses: Array[Class[_]] =
    Array(classOf[RangeInfo],
      //      classOf[PartitionInfo],
      classOf[RowData],
      classOf[GlobalIndexPointData],
      classOf[QuadTree],
      QuadTreeOperations.getClass,
      classOf[GridOperation],
      Helper.getClass,
      classOf[Node[_]],
      classOf[SortedList[_]],
      classOf[Box],
      classOf[Point],
      classOf[PointBase],
      //      classOf[AssignToPartitions],
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

    val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

    val leftBot = gridOp.computeBoxXY(mbrDS1._1, mbrDS1._2)
    val rightTop = gridOp.computeBoxXY(mbrDS1._3, mbrDS1._4)
    val pointHalfXY = new Point(((rightTop._1 - leftBot._1) + 1) / 2.0, ((rightTop._2 - leftBot._2) + 1) / 2.0)

    val globalIndex = new QuadTree(Box(new PointBase(pointHalfXY.x + leftBot._1, pointHalfXY.y + leftBot._2), pointHalfXY))

    var lstRangeInfo = ListBuffer[RangeInfo]()
    var rangeInfo: RangeInfo = null

    var partCounter = 0

    rddRight
      .mapPartitions(_.map(point => (gridOp.computeBoxXY(point.x, point.y), 1L)))
      .reduceByKey(_ + _)
      .collect
      .sortBy(_._1)
      .foreach(row => {

        if (rangeInfo == null || rangeInfo.totalWeight + row._2 > execRowCapacity) {

          rangeInfo = RangeInfo(partCounter, row)
          lstRangeInfo.append(rangeInfo)
          partCounter += 1
        }

        rangeInfo.lstBoxCoords.append(row)
        rangeInfo.totalWeight += row._2
        rangeInfo.rightObj = row

        if (row._1._2 < rangeInfo.bottomObj._1._2) rangeInfo.bottomObj = row
        else if (row._1._2 > rangeInfo.topObj._1._2) rangeInfo.topObj = row
      })

    //    lstRangeInfo.foreach(rInf => println(">1>\t%d\t%d\t%d\t%d\t%d".format(rInf.assignedPartition, rInf.left, rInf.bottom, rInf.right, rInf.top)))

    lstRangeInfo.foreach(rInfo => rInfo.lstBoxCoords
      .foreach(row => globalIndex.insert(new Point(row._1._1, row._1._2, GlobalIndexPointData(row._2, rInfo.assignedPartition)))))

    val bvGlobalIndex = rddRight.sparkContext.broadcast(globalIndex)
    val bvArrMBR = rddRight.sparkContext.broadcast(lstRangeInfo.map(_.mbr).toArray)

    val actualPartitionCount = bvArrMBR.value.length

    val rddSpIdx = rddRight
      .mapPartitions(_.map(point => (point, Byte.MinValue)))
      .partitionBy(new Partitioner() {

        override def numPartitions: Int = actualPartitionCount

        override def getPartition(key: Any): Int = key match {
          case point: Point =>
            QuadTreeOperations.findExact(bvGlobalIndex.value, gridOp.computeBoxXY(point.xy)).userData match {
              case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
            }
        }
      })
      .mapPartitionsWithIndex((pIdx, iter) => {

        val mbr = bvArrMBR.value(pIdx)

        val minX = mbr._1 * gridOp.getBoxWH
        val minY = mbr._2 * gridOp.getBoxWH
        val maxX = mbr._3 * gridOp.getBoxWH + gridOp.getBoxWH
        val maxY = mbr._4 * gridOp.getBoxWH + gridOp.getBoxWH

        val halfWidth = (maxX - minX) / 2
        val halfHeight = (maxY - minY) / 2

        val spatialIndex = new QuadTree(Box(new Point(halfWidth + minX, halfHeight + minY), new Point(halfWidth, halfHeight)))
        iter.foreach(row => spatialIndex.insert(row._1))

        //        println(">2>\t%d\t%s".format(pIdx, spatialIndex.toString()))

        Iterator(spatialIndex)
      } /*, preservesPartitioning = true*/)
      .persist(StorageLevel.MEMORY_ONLY)

    //    lstRangeInfo.foreach(row => println(">2>\t%s".format(row)))

    //    println(">2>=====================================")
    rangeInfo = null
    lstRangeInfo = null

    var rddPoint = rddLeft
      .mapPartitions(_.map(point => {

        //          if (point.userData.toString().equalsIgnoreCase("yellow_3_b_199313"))
        //            println

        val lstPartitionId = QuadTreeOperations.spatialIdxRangeLookup(bvGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k)
          .toList

        val rowPointData: Any = new RowData(point, SortedList[Point](k, allowDuplicates = false), lstPartitionId.tail)

        (lstPartitionId.head, rowPointData)
      }))

    val numRounds = rddLeft
      .mapPartitions(iter => iter.map(point => (gridOp.computeBoxXY(point.x, point.y), Byte.MinValue)))
      .reduceByKey((x, _) => x)
      .mapPartitions(iter => Iterator(iter.map(row => QuadTreeOperations.spatialIdxRangeLookup(bvGlobalIndex.value, row._1, k).size).max))
      .max

    (0 until numRounds).foreach(_ => {

      rddPoint = rddSpIdx
        .mapPartitionsWithIndex((pIdx, iter) => iter.map(qTree => {

          val any: Any = qTree

          (pIdx, any)
        }), preservesPartitioning = true)
        .union(rddPoint)
        .partitionBy(new Partitioner() {

          override def numPartitions: Int = actualPartitionCount

          override def getPartition(key: Any): Int =
            key match {
              case partId: Int => partId
            }
        })
        .mapPartitions(iter => {

          var quadTree: QuadTree = null

          iter.map(row =>
            row._2 match {

              case qt: QuadTree =>

                quadTree = qt

                null
              case rowPointData: RowData =>

                if (rowPointData.lstPartitionId == null)
                  row
                else {

                  //                  if (rowPointData.point.userData.toString().equalsIgnoreCase("Yellow_3_A_239"))
                  //                    println

                  QuadTreeOperations.nearestNeighbor(quadTree, rowPointData.point, rowPointData.sortedList, k)

                  // randomize order to lessen query skews
                  val nextPIdx = if (rowPointData.lstPartitionId.isEmpty) row._1 else rowPointData.lstPartitionId.head
                  rowPointData.lstPartitionId = if (rowPointData.lstPartitionId.isEmpty) null else Random.shuffle(rowPointData.lstPartitionId.tail)

                  (nextPIdx, rowPointData)
                }
            })
            .filter(_ != null)
        })
    })

    rddPoint.mapPartitions(_.map(_._2 match { case rowPointData: RowData => rowPointData })
      .map(rowPointData => (rowPointData.point, rowPointData.sortedList.map(nd => (nd.distance, nd.data)))))
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
    val quadTreeEmptyDummy = new QuadTree(Box(new Point(pointDummy), new Point(pointDummy)))
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
}