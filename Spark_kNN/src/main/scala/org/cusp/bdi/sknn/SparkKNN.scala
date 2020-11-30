package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Circle, Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.{KdTree, KdtBranchRootNode, KdtNode}
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.sortset.{Node, SortedList}
import org.cusp.bdi.sknn.ds.util.{PointData, SpatialIdxRangeLookup}
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.{Arguments, Helper}

import scala.collection.mutable.ListBuffer
import scala.util.Random

final class RangeInfo extends Serializable {

  val lstMBRCoord: ListBuffer[((Double, Double), Int)] = ListBuffer[((Double, Double), Int)]()
  var totalWeight = 0
  var left: Double = _
  var bottom: Double = _
  var right: Double = _
  var top: Double = _

  def this(startCoord: (Double, Double), count: Int) = {

    this()

    this.lstMBRCoord += ((startCoord, count))
    this.totalWeight = count
    this.left = startCoord._1
    this.bottom = startCoord._2
    this.right = startCoord._1
    this.top = startCoord._2
  }

  override def toString: String =
    "%f\t%f\t%f\t%f\t%d".format(left, bottom, right, top, totalWeight)
}

final case class RowData(point: Point, sortedList: SortedList[Point]) extends Serializable {

  var lstPartitionId: ListBuffer[Int] = _

  //  def this(point: Point, sortedList: SortedList[Point], lstPartitionId: ListBuffer[Int]) {
  //
  //    this(point, sortedList)
  //
  //    this.lstPartitionId = lstPartitionId
  //  }
}

case class GlobalIndexPointData(numPoints: Int, partitionIdx: Int) extends PointData {
  override def equals(other: Any): Boolean = false
}

object SparkKNN extends Serializable {

  def getSparkKNNClasses: Array[Class[_]] =
    Array(
      Helper.getClass,
      classOf[SortedList[_]],
      classOf[Rectangle],
      classOf[Circle],
      classOf[Point],
      classOf[Geom2D],
      classOf[QuadTree],
      classOf[KdTree],
      classOf[KdtNode],
      classOf[KdtBranchRootNode],
      SparkKNN.getClass,
      classOf[SparkKNN],
      classOf[RowData],
      classOf[RangeInfo],
      TypeSpatialIndex.getClass,
      classOf[GlobalIndexPointData],
      classOf[SpatialIndex],
      classOf[GridOperation],
      Arguments.getClass,
      classOf[Node[_]])
}

case class SparkKNN(debugMode: Boolean, typeSpatialIndex: TypeSpatialIndex.Value, k: Int) extends Serializable {

  def knnJoin(rddLeft: RDD[Point], rddRight: RDD[Point]): RDD[(Point, Iterable[(Double, Point)])] = {

    val (rSetPartObjectCapacity, rSetRowCount, rSetMBR) = computeCapacity(rddRight)

    val rGlobalIndex = SpatialIndex(typeSpatialIndex)

    knnJoinExecute(rddLeft, rddRight, rSetRowCount, rSetMBR, rSetPartObjectCapacity, rGlobalIndex, null)
  }

  def allKnnJoin(rddLeft: RDD[Point], rddRight: RDD[Point]): RDD[(Point, Iterable[(Double, Point)])] = {

    val (rSetPartObjectCapacity, rSetRowCount, rSetMBR) = computeCapacity(rddRight)
    val (lSetPartObjectCapacity, lSetRowCount, lSetMBR) = computeCapacity(rddRight)

    val setPartObjectCapacity = rSetPartObjectCapacity.min(lSetPartObjectCapacity) / 2 + 1

    val rGlobalIndex = SpatialIndex(typeSpatialIndex)
    val join1 = knnJoinExecute(rddLeft, rddRight, rSetRowCount, rSetMBR, setPartObjectCapacity, rGlobalIndex, null)

    val lGlobalIndex = SpatialIndex(typeSpatialIndex)
    val join2 = knnJoinExecute(rddRight, rddLeft, lSetRowCount, lSetMBR, setPartObjectCapacity, rGlobalIndex, lGlobalIndex)

    join1.union(join2)
  }

  private def knnJoinExecute(rddLeft: RDD[Point], rddRight: RDD[Point], rSetRowCount: Long, rSetMBR: (Double, Double, Double, Double), partObjectCapacity: Int, rGlobalIndex: SpatialIndex, lGlobalIndex: SpatialIndex): RDD[(Point, Iterable[(Double, Point)])] = {

    val bvGridOperation = rddRight.context.broadcast(new GridOperation(rSetMBR, rSetRowCount, k))

    val rectGlobalIdx = buildRectBounds(bvGridOperation.value.computeSquareXY(rSetMBR._1, rSetMBR._2), bvGridOperation.value.computeSquareXY(rSetMBR._3, rSetMBR._4))

    var startTime = System.currentTimeMillis
    var actualPartitionCount = -1
    var bvGlobalIndex: Broadcast[SpatialIndex] = null
    var bvArrPartitionsMBRs: Broadcast[Array[(Double, Double, Double, Double)]] = null

    {
      val lstRangeInfo = ListBuffer[RangeInfo]()
      var rangeInfo: RangeInfo = null

      // build range info
      rddRight
        .mapPartitions(_.map(point => (bvGridOperation.value.computeSquareXY(point.x, point.y), 1L))) // grid assignment
        .reduceByKey(_ + _) // summarize
        .sortByKey()
        .collect()
        .foreach(row => { // group cells on partitions

          val count = if (row._2 > Int.MaxValue) Int.MaxValue else row._2.toInt

          if (rangeInfo == null || rangeInfo.totalWeight + count > partObjectCapacity) {

            rangeInfo = new RangeInfo(row._1, count)
            lstRangeInfo += rangeInfo
          }
          else {

            rangeInfo.lstMBRCoord.append((row._1, count))
            rangeInfo.totalWeight += count
            rangeInfo.right = row._1._1

            if (row._1._2 < rangeInfo.bottom)
              rangeInfo.bottom = row._1._2
            else if (row._1._2 > rangeInfo.top)
              rangeInfo.top = row._1._2
          }
        })

      Helper.loggerSLf4J(debugMode, SparkKNN, ">>rangeInfo time in %,d MS".format(System.currentTimeMillis - startTime))

      startTime = System.currentTimeMillis

      // create global index
      rGlobalIndex.insert(rectGlobalIdx, lstRangeInfo.map(rangeInfo => {

        actualPartitionCount += 1

        rangeInfo
          .lstMBRCoord
          .map(row => new Point(row._1._1, row._1._2, GlobalIndexPointData(row._2, actualPartitionCount)))
      })
        .flatMap(_.seq).iterator, 1)

      actualPartitionCount += 1

      bvGlobalIndex = rddRight.context.broadcast(rGlobalIndex)
      bvArrPartitionsMBRs = rddRight.context.broadcast(lstRangeInfo.map(rInf => (rInf.left, rInf.bottom, rInf.right, rInf.top)).toArray)
    }

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>GlobalIndex insert time in %,d MS. Index: %s\tTotal Size: %,d".format(System.currentTimeMillis - startTime, rGlobalIndex, SizeEstimator.estimate(rGlobalIndex)))

    //    val startTime = System.currentTimeMillis

    // build a spatial index on each partition
    val rddSpIdx = rddRight
      .mapPartitions(_.map(point => {

//        if (point.userData.toString.equalsIgnoreCase("Taxi_2_B_649193"))
//          println(bvGlobalIndex.value.findExact(bvGridOperation.value.computeSquareXY(point.x, point.y)).userData match {
//            case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
//          })

        (bvGlobalIndex.value.findExact(bvGridOperation.value.computeSquareXY(point.x, point.y)).userData match {
          case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
        }, point)
      })
      )
      .partitionBy(new Partitioner() { // group points

        override def numPartitions: Int = actualPartitionCount

        override def getPartition(key: Any): Int = key match {
          case pIdx: Int => // also used when partitioning rddPoint
            if (pIdx == -1) Random.nextInt(numPartitions)
            else pIdx
        }
      })
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        //        val startTime = System.currentTimeMillis
        val mbr = bvArrPartitionsMBRs.value(pIdx)

        val minX = mbr._1 * bvGridOperation.value.squareDim
        val minY = mbr._2 * bvGridOperation.value.squareDim
        val maxX = mbr._3 * bvGridOperation.value.squareDim + bvGridOperation.value.squareDim
        val maxY = mbr._4 * bvGridOperation.value.squareDim + bvGridOperation.value.squareDim

        val rectBounds = buildRectBounds((minX, minY), (maxX, maxY))

        val spatialIndex: SpatialIndex = SpatialIndex(typeSpatialIndex)

        spatialIndex.insert(rectBounds, iter.map(_._2), bvGridOperation.value.squareDim)

        //        Helper.loggerSLf4J(debugMode, SparkKNN, ">>SpatialIndex on partition %d time in %,d MS. Index: %s. Total Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, SizeEstimator.estimate(spatialIndex)))

        Iterator((pIdx, spatialIndex.asInstanceOf[Any]))
      }
        , preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_ONLY)

    //    val startTime = System.currentTimeMillis

    val numRounds =
      if (lGlobalIndex == null)
        rddLeft
          .mapPartitions(_.map(point => bvGridOperation.value.computeSquareXY(point.x, point.y)).toSet.iterator)
          .distinct
          .mapPartitions(iter => Iterator(iter.map(row => SpatialIdxRangeLookup.getLstPartition(bvGlobalIndex.value, row, k).length).max))
          .max
      else
        lGlobalIndex.map(_.map(point => SpatialIdxRangeLookup.getLstPartition(bvGlobalIndex.value, point.xy, k).length).max).max

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>LeftDS numRounds time in %,d MS, numRounds: %d".format(System.currentTimeMillis - startTime, numRounds))

    var rddPoint = rddLeft
      .mapPartitions(iter => iter.map(point => {

        var lstPartitionId = SpatialIdxRangeLookup.getLstPartition(bvGlobalIndex.value, bvGridOperation.value.computeSquareXY(point.x, point.y), k)

//        if (point.userData.toString.equalsIgnoreCase("taxi_2_a_827408"))
//          println(SpatialIdxRangeLookup.getLstPartition(bvGlobalIndex.value, bvGridOperation.value.computeSquareXY(point.x, point.y), k))

        while (lstPartitionId.length < numRounds)
          lstPartitionId += -1

        lstPartitionId = Random.shuffle(lstPartitionId)

        val rDataPoint = RowData(point, SortedList[Point](k))
        rDataPoint.lstPartitionId = lstPartitionId.tail

        (lstPartitionId.head, rDataPoint.asInstanceOf[Any])
      }))

    (0 until numRounds).foreach(_ => {
      rddPoint = rddSpIdx
        .union(new ShuffledRDD(rddPoint, rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          val spatialIndex = iter.next._2 match {
            case spIdx: SpatialIndex => spIdx
          }

          iter.map(row => row._2 match {
            case rowData: RowData =>

              if (row._1 != -1) {

//                if (rowData.point.userData.toString.equalsIgnoreCase("taxi_2_a_827408"))
//                  println(pIdx)

                spatialIndex.nearestNeighbor(rowData.point, rowData.sortedList)
              }

              val nextPIdx = if (rowData.lstPartitionId.isEmpty) -1 else rowData.lstPartitionId.head

              rowData.lstPartitionId = if (rowData.lstPartitionId.isEmpty) null else rowData.lstPartitionId.tail

              (nextPIdx, rowData)
          })
        })

      bvGridOperation.unpersist()
      bvGlobalIndex.unpersist()
      bvArrPartitionsMBRs.unpersist()
    })

    rddPoint.map(_._2 match {
      case rowData: RowData => (rowData.point, rowData.sortedList.map(nd => (nd.distance, nd.data)))
    })
  }

  private def computeCapacity(rdd: RDD[Point]): (Int, Long, (Double, Double, Double, Double)) = {

    var startTime = System.currentTimeMillis

    val (rightDS_MaxRowSize, setRowCount, mbrLeft, mbrBottom, mbrRight, mbrTop) =
      rdd.mapPartitions(iter => {

        var maxRowLen = 0
        var rowCount = 0L
        var minX = Double.MaxValue
        var minY = Double.MaxValue
        var maxX = Double.MinValue
        var maxY = Double.MinValue

        while (iter.hasNext) {

          val point = iter.next

          rowCount += 1

          if (point.userData.toString.length > maxRowLen) maxRowLen = point.userData.toString.length

          if (point.x < minX) minX = point.x
          if (point.y < minY) minY = point.y
          if (point.x > maxX) maxX = point.x
          if (point.y > maxY) maxY = point.y
        }

        Iterator((maxRowLen, rowCount, minX, minY, maxX, maxY))
      })
        .reduce((param1, param2) =>
          (param1._1.max(param2._1), param1._2 + param2._2, param1._3.min(param2._3), param1._4.min(param2._4), param1._5.max(param2._5), param1._6.max(param2._6)))

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>Right DS info time in %,d MS".format(System.currentTimeMillis - startTime))
    Helper.loggerSLf4J(debugMode, SparkKNN, ">>rightDS_MaxRowSize:%d\tsetRowCount:%d\tmbrLeft:%.8f\tmbrBottom:%.8f\tmbrRight:%.8f\tmbrTop:%.8f".format(rightDS_MaxRowSize, setRowCount, mbrLeft, mbrBottom, mbrRight, mbrTop))

    startTime = System.currentTimeMillis

    // 7% reduction in memory to account for overhead operations
    val execAvailableMemory = Helper.toByte(rdd.context.getConf.get("spark.executor.memory", rdd.context.getExecutorMemoryStatus.map(_._2._1).max + "B")) // skips memory of core assigned for Hadoop daemon
    val exeOverheadMemory = (0.1 * execAvailableMemory).max(384).toLong + 1 // deduct yarn overhead
    val coresPerExec = rdd.context.getConf.getInt("spark.executor.cores", 1)
    val coreAvailableMemory = (execAvailableMemory - exeOverheadMemory - exeOverheadMemory) / coresPerExec

    val rectDummy = Rectangle(new Geom2D(0, 0), new Geom2D(0, 0))
    val pointDummy = new Point(0, 0, Array.fill[Char](rightDS_MaxRowSize)(' ').mkString(""))
    val sortSetDummy = SortedList[Point](k)
    val lstPartitionIdDummy = ListBuffer.fill[Int](rdd.getNumPartitions)(0)
    val rowDataDummy = RowData(pointDummy, sortSetDummy)
    rowDataDummy.lstPartitionId = lstPartitionIdDummy
    val spatialIndexDummy = SpatialIndex(typeSpatialIndex)

    val pointCost = SizeEstimator.estimate(pointDummy)
    val rowDataCost = SizeEstimator.estimate(rowDataDummy) + (pointCost * k)
    val spatialIndexCost = SizeEstimator.estimate(spatialIndexDummy) + SizeEstimator.estimate(rectDummy)

    // exec mem cost = 1QT + 1Pt and matches
    var partObjectCapacity = (coreAvailableMemory - spatialIndexCost - rowDataCost) / pointCost

    val numParts = if (setRowCount < partObjectCapacity) rdd.getNumPartitions
    else setRowCount / partObjectCapacity + 1 //, (rddRight.context.getExecutorMemoryStatus.size - 1) * coresPerExec)

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>execAvailableMemory=%d\texeOverheadMemory=%d\tcoresPerExec=%d\tcoreAvailableMemory=%d\tpointCost=%d\trowDataCost=%d\tspatialIndexCost=%d\tpartObjectCapacity=%d\tnumParts=%d".format(execAvailableMemory, exeOverheadMemory, coresPerExec, coreAvailableMemory, pointCost, rowDataCost, spatialIndexCost, partObjectCapacity, numParts))

    partObjectCapacity = (setRowCount / numParts) + 1

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>partObjectCapacity time in %,d MS".format(System.currentTimeMillis - startTime))
    Helper.loggerSLf4J(debugMode, SparkKNN, ">>partObjectCapacity:%d".format(partObjectCapacity))

    if (partObjectCapacity > Int.MaxValue)
      partObjectCapacity = Int.MaxValue

    (partObjectCapacity.toInt, setRowCount, (mbrLeft, mbrBottom, mbrRight, mbrTop))
  }
}