package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.kt.KdTree
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.{Box, Circle, Point, PointBase}
import org.cusp.bdi.sknn.ds.util.{KdTree_kNN, QuadTree_kNN, SpatialIndex_kNN}
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.{Helper, Node, SortedList}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object TypeSpatialIndex extends Enumeration {

  val quadTree = Value(0.toByte)
  val kdTree = Value(1.toByte)
}

case class RangeInfo(seedObj: ((Double, Double), Long)) extends Serializable {

  val lstBoxCoords = ListBuffer[((Double, Double), Long)]()
  var totalWeight = 0L
  val leftObj = seedObj
  var bottomObj = seedObj
  var rightObj = seedObj
  var topObj = seedObj

  def mbr = (leftObj._1._1, bottomObj._1._2, rightObj._1._1, topObj._1._2)

  override def toString: String =
    "%f\t%f\t%f\t%f\t%d".format(leftObj._1._1, bottomObj._1._2, rightObj._1._1, topObj._1._2, totalWeight)
}

case class RowData(point: Point, sortedList: SortedList[Point]) {

  var lstPartitionId: List[Int] = _

  def this(point: Point, sortedList: SortedList[Point], _lstPartitionId: List[Int]) {

    this(point, sortedList)

    this.lstPartitionId = _lstPartitionId
  }
}

case class GlobalIndexPointData(numPoints: Long, partitionIdx: Int) extends Serializable {}

object SparkKNN {

  def getSparkKNNClasses: Array[Class[_]] =
    Array(
      classOf[RowData],
      classOf[GlobalIndexPointData],
      classOf[RangeInfo],
      TypeSpatialIndex.getClass,
      classOf[Box],
      classOf[Circle],
      classOf[Point],
      classOf[PointBase],
      classOf[QuadTree],
      classOf[KdTree],
      classOf[QuadTree_kNN],
      classOf[KdTree_kNN],
      classOf[SparkKNN],
      classOf[SpatialIndex_kNN],
      classOf[GridOperation],
      RDD_Store.getClass,
      SparkKNN_Arguments.getClass,
      Helper.getClass,
      classOf[Node[_]],
      classOf[SortedList[_]],
      RDD_Store.getClass,
      SparkKNN_Arguments.getClass)
}

case class SparkKNN(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int, typeSpatialIndex: TypeSpatialIndex.Value) {

  // for testing, remove ...
  var minPartitions = 0

  def knnJoin() =
    knnJoinExecute(rddLeft, rddRight, isAllJoin = false)

  def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] =
    knnJoinExecute(rddLeft, rddRight, isAllJoin = true)
      .union(knnJoinExecute(rddRight, rddLeft, isAllJoin = false))

  var (ds2MaxRowSize, ds2TotalRowCount, ds2MBR_Left, ds2MBR_Bottom, ds2MBR_Right, ds2MBR_Top) = (-1, -1L, -1.0, -1.0, -1.0, -1.0)

  private def knnJoinExecute(rddLeft: RDD[Point], rddRight: RDD[Point], isAllJoin: Boolean): RDD[(Point, Iterable[(Double, Point)])] = {

    val (ds1MaxRowSize, ds1TotalRowCount, ds1MBR_Left, ds1MBR_Bottom, ds1MBR_Right, ds1MBR_Top) =
      if (ds2MaxRowSize == -1)
        rddRight.mapPartitions(iter =>
          Iterator(iter.map(point => (point.userData.toString.length, 1L, point.x, point.y, point.x, point.y))
            .fold(0, 0L, Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)((param1, param2) =>
              (math.max(param1._1, param2._1), param1._2 + param2._2, math.min(param1._3, param2._3), math.min(param1._4, param2._4), math.max(param1._5, param2._5), math.max(param1._6, param2._6)))
          ))
          .fold(0, 0L, Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)((param1, param2) =>
            (math.max(param1._1, param2._1), param1._2 + param2._2, math.min(param1._3, param2._3), math.min(param1._4, param2._4), math.max(param1._5, param2._5), math.max(param1._6, param2._6)))
      else
        (ds2MaxRowSize, ds2TotalRowCount, ds2MBR_Left, ds2MBR_Bottom, ds2MBR_Right, ds2MBR_Top)

    val execRowCapacity = computeCapacity(rddRight, ds1MaxRowSize, ds1TotalRowCount)

    //        execRowCapacity = 57702
    //        println(">>" + execRowCapacity)

    val gridOp = new GridOperation(ds1MBR_Left, ds1MBR_Bottom, ds1MBR_Right, ds1MBR_Top, ds1TotalRowCount, k)

    var lstRangeInfo = ListBuffer[RangeInfo]()
    var rangeInfo: RangeInfo = null

    rddRight
      .mapPartitions(_.map(point => (gridOp.computeBoxXY(point.x, point.y), 1L))) // grid assignment
      .reduceByKey(_ + _) // summarize
      .collect
      .sortBy(_._1) // sorts by column then row ((0, 0), (0, 1), ..., (1, 0), (1, 1) ...)
      .foreach(row => { // group cells on partitions

        if (rangeInfo == null || rangeInfo.totalWeight + row._2 > execRowCapacity) {

          rangeInfo = RangeInfo(row)
          lstRangeInfo.append(rangeInfo)
        }

        rangeInfo.lstBoxCoords.append(row)
        rangeInfo.totalWeight += row._2
        rangeInfo.rightObj = row

        if (row._1._2 < rangeInfo.bottomObj._1._2) rangeInfo.bottomObj = row
        else if (row._1._2 > rangeInfo.topObj._1._2) rangeInfo.topObj = row
      })

    //    lstRangeInfo.foreach(rInf => println(">1>\t%d\t%d\t%d\t%d\t%d".format(rInf.assignedPartition, rInf.left, rInf.bottom, rInf.right, rInf.top)))

    val globalIndex: SpatialIndex_kNN = typeSpatialIndex match {
      case qt if qt == TypeSpatialIndex.quadTree =>
        new QuadTree_kNN(gridOp.computeBoxXY(ds1MBR_Left, ds1MBR_Bottom), gridOp.computeBoxXY(ds1MBR_Right, ds1MBR_Top))
      case kdt if kdt == TypeSpatialIndex.kdTree =>
        new KdTree_kNN()
    }

    var arrRangeInfo = lstRangeInfo.toArray
    lstRangeInfo = null

    arrRangeInfo.indices
      .foreach(idx => arrRangeInfo(idx).lstBoxCoords
        .foreach(row => globalIndex.insert(new Point(row._1._1, row._1._2, GlobalIndexPointData(row._2, idx)))))

    val bvGlobalIndex = rddRight.sparkContext.broadcast(globalIndex)
    val bvArrMBR = rddRight.sparkContext.broadcast(arrRangeInfo.map(_.mbr))

    val actualPartitionCount = arrRangeInfo.length

    rangeInfo = null
    arrRangeInfo = null

    val rddSpIdx = rddRight
      .mapPartitions(_.map(point => (point, Byte.MinValue)))
      .partitionBy(new Partitioner() { // group points

        override def numPartitions: Int = actualPartitionCount

        override def getPartition(key: Any): Int = key match {
          case point: Point =>
            bvGlobalIndex.value.findExact(gridOp.computeBoxXY(point.xy)).userData match {
              case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
            }
        }
      })
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        val spatialIndex: SpatialIndex_kNN = typeSpatialIndex match {
          case qt if qt == TypeSpatialIndex.quadTree => new QuadTree_kNN(bvArrMBR.value(pIdx), gridOp.getBoxWH)
          case kdt if kdt == TypeSpatialIndex.kdTree => new KdTree_kNN()
        }

        iter.foreach(row => spatialIndex.insert(row._1))

        //        println(">2>\t%d\t%s".format(pIdx, spatialIndex.toString()))

        val any: Any = spatialIndex

        Iterator((pIdx, any))
      }, preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_ONLY)

    //    lstRangeInfo.foreach(row => println(">2>\t%s".format(row)))
    //    println(">2>=====================================")

    var numRounds = if (isAllJoin) {

      val (numR, (maxRowSize, totalRowCount, mbrLeft, mbrBott, mbrRight, mbrTop)) = rddLeft.mapPartitions(_.map(point => (gridOp.computeBoxXY(point.x, point.y), (point.userData.toString.length, 1L, point.x, point.y, point.x, point.y))))
        .reduceByKey((param1, param2) =>
          (math.max(param1._1, param2._1), param1._2 + param2._2, math.min(param1._3, param2._3), math.min(param1._4, param2._4), math.max(param1._5, param2._5), math.max(param1._6, param2._6)))
        .mapPartitions(iter =>
          Iterator(iter.map(row => (bvGlobalIndex.value.spatialIdxRangeLookup(row._1, k).size, row._2))
            .fold(0, (0, 0L, Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))((param1, param2) =>
              (math.max(param1._1, param2._1), (math.max(param1._2._1, param2._2._1), param1._2._2 + param2._2._2, math.min(param1._2._3, param2._2._3), math.min(param1._2._4, param2._2._4), math.max(param1._2._5, param2._2._5), math.max(param1._2._6, param2._2._6)))
            ))
        )
        .fold(0, (0, 0L, Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))((param1, param2) =>
          (math.max(param1._1, param2._1), (math.max(param1._2._1, param2._2._1), param1._2._2 + param2._2._2, math.min(param1._2._3, param2._2._3), math.min(param1._2._4, param2._2._4), math.max(param1._2._5, param2._2._5), math.max(param1._2._6, param2._2._6))))

      ds2MaxRowSize = maxRowSize
      ds2TotalRowCount = totalRowCount
      ds2MBR_Left = mbrLeft
      ds2MBR_Bottom = mbrBott
      ds2MBR_Right = mbrRight
      ds2MBR_Top = mbrTop

      numR
    }
    else
      rddLeft
        .mapPartitions(_.map(point => gridOp.computeBoxXY(point.x, point.y)).toSet.iterator)
        .distinct()
        .mapPartitions(iter => Iterator(iter.map(gridXY => bvGlobalIndex.value.spatialIdxRangeLookup(gridXY, k).size).max))
        .max

    var rddPoint = rddLeft
      .mapPartitions(_.map(point => {

        //          if (point.userData.toString().equalsIgnoreCase("yellow_3_b_199313"))
        //            println

        val lstPartitionId = bvGlobalIndex.value.spatialIdxRangeLookup(gridOp.computeBoxXY(point.x, point.y), k)
          .toList

        val rowPointData: Any = new RowData(point, SortedList[Point](k, allowDuplicates = false), Random.shuffle(lstPartitionId.tail))

        (lstPartitionId.head, rowPointData)
      }))

    (0 until numRounds).foreach(_ =>
      rddPoint = rddSpIdx
        .union(rddPoint)
        .partitionBy(new Partitioner() {

          override def numPartitions: Int = actualPartitionCount

          override def getPartition(key: Any): Int =
            key match {
              case partId: Int => partId
            }
        })
        .mapPartitions(iter => {

          var spatialIndex: SpatialIndex_kNN = null

          iter.map(row =>
            row._2 match {

              case spIdx: SpatialIndex_kNN =>

                spatialIndex = spIdx

                null
              case rowPointData: RowData =>

                if (rowPointData.lstPartitionId == null)
                  (Random.nextInt(actualPartitionCount), row._2)
                else {

                  //                  if (rowPointData.point.userData.toString().equalsIgnoreCase("Yellow_3_A_239"))
                  //                    println

                  spatialIndex.nearestNeighbor(rowPointData.point, rowPointData.sortedList, k)

                  // randomize order to lessen query skews
                  val nextPIdx = if (rowPointData.lstPartitionId.isEmpty) row._1 else rowPointData.lstPartitionId.head
                  rowPointData.lstPartitionId = if (rowPointData.lstPartitionId.isEmpty) null else Random.shuffle(rowPointData.lstPartitionId.tail)

                  (nextPIdx, rowPointData)
                }
            })
            .filter(_ != null)
        })
    )

    rddPoint.mapPartitions(_.map(_._2 match { case rowPointData: RowData => rowPointData })
      .map(rowPointData => (rowPointData.point, rowPointData.sortedList.map(nd => (nd.distance, nd.data)))))
  }

  private def computeCapacity(rddRight: RDD[Point], maxRowSize: Int, totalRowCount: Long) = {

    // 7% reduction in memory to account for overhead operations
    val execAvailableMemory = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory", rddRight.context.getExecutorMemoryStatus.map(_._2._1).max + "B")) // skips memory of core assigned for Hadoop daemon
    // deduct yarn overhead
    val exeOverheadMemory = math.max(384, 0.1 * execAvailableMemory).toLong + 1

    //        val gmGeomDummy = GMPoint((0 until maxRowSize).map(_ => " ").mkString(""), (0, 0))
    val userData = Array.fill[Char](maxRowSize)(' ').mkString("Array(", ", ", ")")
    val pointDummy = new Point(0, 0, userData)
    val quadTreeEmptyDummy = typeSpatialIndex match {
      case qt if qt == TypeSpatialIndex.quadTree =>
        new QuadTree_kNN(Box(new PointBase(pointDummy), new PointBase(pointDummy)))
      case kdt if kdt == TypeSpatialIndex.kdTree =>
        new KdTree_kNN()
    }

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

    execRowCapacity
  }
}