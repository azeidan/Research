package org.cusp.bdi.sknn

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Circle, Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.{KdTree, KdtBranchRootNode, KdtLeafNode, KdtNode}
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.sortset.{Node, SortedList}
import org.cusp.bdi.sknn.ds.util.{GlobalIndexPointData, SpatialIdxRangeLookup}
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.Helper

import scala.collection.mutable.ListBuffer
import scala.util.Random

final class RangeInfo {

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

  def mbr: (Double, Double, Double, Double) =
    (left, bottom, right, top)

  override def toString: String =
    "%f\t%f\t%f\t%f\t%d".format(left, bottom, right, top, totalWeight)
}

final class RowData extends KryoSerializable {

  var point: Point = _
  var sortedList: SortedList[Point] = _
  var lstPartitionId: ListBuffer[Int] = _

  def this(point: Point, sortedList: SortedList[Point], lstPartitionId: ListBuffer[Int]) = {

    this()

    this.point = point
    this.sortedList = sortedList
    this.lstPartitionId = lstPartitionId
  }

  override def write(kryo: Kryo, output: Output): Unit = {

    kryo.writeClassAndObject(output, point)
    kryo.writeClassAndObject(output, sortedList)
    kryo.writeClassAndObject(output, lstPartitionId)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    point = kryo.readClassAndObject(input) match {
      case pt: Point => pt
    }

    sortedList = kryo.readClassAndObject(input).asInstanceOf[SortedList[Point]]
    lstPartitionId = kryo.readClassAndObject(input).asInstanceOf[ListBuffer[Int]]
  }
}

case class BroadcastWrapper(spatialIdx: SpatialIndex, gridOp: GridOperation, arrPartitionMBRs: Array[(Double, Double, Double, Double)]) extends Serializable {}

object SparkKNN extends Serializable {

  def getSparkKNNClasses: Array[Class[_]] =
    Array(
      Helper.getClass,
      classOf[SortedList[_]],
      classOf[Rectangle],
      classOf[Circle],
      classOf[Point],
      classOf[Geom2D],
      classOf[GridOperation],
      classOf[KdTree],
      KdTree.getClass,
      classOf[QuadTree],
      classOf[KdTree],
      classOf[KdtNode],
      classOf[KdtBranchRootNode],
      classOf[KdtLeafNode],
      classOf[SparkKNN],
      BroadcastWrapper.getClass,
      SparkKNN.getClass,
      SupportedSpatialIndexes.getClass,
      classOf[SparkKNN],
      classOf[RangeInfo],
      SpatialIndex.getClass,
      classOf[SpatialIndex],
      classOf[GridOperation],
      classOf[Node[_]])
}

case class SparkKNN(debugMode: Boolean, spatialIndexType: SupportedSpatialIndexes.Value, k: Int) extends Serializable {

  def knnJoin(rddLeft: RDD[Point], rddRight: RDD[Point]): RDD[(Point, Iterable[(Double, Point)])] = {

    val (partObjCapacityRight, rowCountRight, mbrRight) = computeCapacity(rddRight, false)

    val gridOpRight = new GridOperation(mbrRight, rowCountRight, k)

    knnJoinExecute(rddLeft, rddRight,
      buildRectBounds(gridOpRight.computeSquareXY(mbrRight._1, mbrRight._2), gridOpRight.computeSquareXY(mbrRight._3, mbrRight._4)),
      gridOpRight, SpatialIndex(spatialIndexType), partObjCapacityRight, null, null)
  }

  def allKnnJoin(rddLeft: RDD[Point], rddRight: RDD[Point]): RDD[(Point, Iterable[(Double, Point)])] = {

    val (partObjCapacityRight, rowCountRight, mbrRight) = computeCapacity(rddRight, true)
    val (partObjCapacityLeft, rowCountLeft, mbrLeft) = computeCapacity(rddLeft, true)

    val partObjCapacity = math.min(partObjCapacityRight, partObjCapacityLeft) / 2
    val rowCount = math.min(rowCountRight, rowCountLeft)

    val spatialIndexRight = SpatialIndex(spatialIndexType)
    val (gridOpRight, gridOpLeft) = (new GridOperation(mbrRight, rowCount, k), new GridOperation(mbrLeft, rowCount, k))

    knnJoinExecute(rddLeft, rddRight,
      buildRectBounds(gridOpRight.computeSquareXY(mbrRight._1, mbrRight._2), gridOpRight.computeSquareXY(mbrRight._3, mbrRight._4)),
      gridOpRight, spatialIndexRight, partObjCapacity, null, null)
      .union(knnJoinExecute(rddRight, rddLeft,
        buildRectBounds(gridOpLeft.computeSquareXY(mbrLeft._1, mbrLeft._2), gridOpLeft.computeSquareXY(mbrLeft._3, mbrLeft._4)),
        gridOpLeft, SpatialIndex(spatialIndexType), partObjCapacity, gridOpRight, spatialIndexRight))
  }

  private def knnJoinExecute(rddLeft: RDD[Point], rddRight: RDD[Point], rectGlobalIdxRight: Rectangle, gridOpRight: GridOperation, spatialIdxRight: SpatialIndex, partObjCapacity: Int, gridOpOther: GridOperation, spatialIdxOther: SpatialIndex): RDD[(Point, Iterable[(Double, Point)])] = {

    var startTime = System.currentTimeMillis
    var bvBroadcastWrapperRight: Broadcast[BroadcastWrapper] = null
    val lstRangeInfo = ListBuffer[RangeInfo]()
    var rangeInfo: RangeInfo = null

    // build range info
    rddRight
      .mapPartitions(iter => iter.map(point => (gridOpRight.computeSquareXY(point.x, point.y), 1L))) // grid assignment
      .reduceByKey(_ + _) // summarize
      .sortByKey()
      .collect()
      .foreach(row => { // group cells on partitions

        val count = if (row._2 > Int.MaxValue) Int.MaxValue else row._2.toInt

        if (rangeInfo == null || rangeInfo.totalWeight + count > partObjCapacity) {

          rangeInfo = new RangeInfo(row._1, count)
          lstRangeInfo += rangeInfo
        }
        else {

          rangeInfo.lstMBRCoord += ((row._1, count))
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
    var actualPartitionCount = -1
    spatialIdxRight.insert(rectGlobalIdxRight, lstRangeInfo.map(rangeInfo => {

      actualPartitionCount += 1

      rangeInfo
        .lstMBRCoord
        .map(row => new Point(row._1._1, row._1._2, new GlobalIndexPointData(row._2, actualPartitionCount)))
    })
      .flatMap(_.seq)
      .view
      .iterator, 1)

    //    actualPartitionCount += 1

    bvBroadcastWrapperRight = rddRight.context.broadcast(BroadcastWrapper(spatialIdxRight, gridOpRight, lstRangeInfo.map(_.mbr).toArray))

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>GlobalIndex insert time in %,d MS. Index: %s\tBroadcast Total Size: %,d".format(System.currentTimeMillis - startTime, bvBroadcastWrapperRight.value.spatialIdx, SizeEstimator.estimate(bvBroadcastWrapperRight.value)))

    //    val startTime = System.currentTimeMillis

    // build a spatial index on each partition
    val rddSpIdx = rddRight
      .mapPartitions(_.map(point => (bvBroadcastWrapperRight.value.spatialIdx.findExact(bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y)).userData match {
        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
      }, point)))
      .partitionBy(new Partitioner() { // group points

        override def numPartitions: Int = bvBroadcastWrapperRight.value.arrPartitionMBRs.length

        override def getPartition(key: Any): Int = key match { // this partitioner is used when partitioning rddPoint
          case pIdx: Int =>
            if (pIdx == -1) Random.nextInt(numPartitions) else pIdx
        }
      })
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        val startTime = System.currentTimeMillis
        val mbr = bvBroadcastWrapperRight.value.arrPartitionMBRs(pIdx)

        val minX = mbr._1 * bvBroadcastWrapperRight.value.gridOp.squareDim
        val minY = mbr._2 * bvBroadcastWrapperRight.value.gridOp.squareDim
        val maxX = mbr._3 * bvBroadcastWrapperRight.value.gridOp.squareDim + bvBroadcastWrapperRight.value.gridOp.squareDim
        val maxY = mbr._4 * bvBroadcastWrapperRight.value.gridOp.squareDim + bvBroadcastWrapperRight.value.gridOp.squareDim

        val rectBounds = buildRectBounds((minX, minY), (maxX, maxY))

        val spatialIndex = SpatialIndex(spatialIndexType)

        spatialIndex.insert(rectBounds, iter.map(_._2), bvBroadcastWrapperRight.value.gridOp.squareDim)

        Helper.loggerSLf4J(debugMode, SparkKNN, ">>SpatialIndex on partition %d time in %,d MS. Index: %s. Total Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, SizeEstimator.estimate(spatialIndex)))

        Iterator((pIdx, spatialIndex.asInstanceOf[Any]))
      }
        , preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_ONLY)

    //    val startTime = System.currentTimeMillis

    val numRounds = if (spatialIdxOther == null)
      rddLeft
        .mapPartitions(_.map(point => bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y)))
        .distinct
        .mapPartitions(_.map(xyCoord => SpatialIdxRangeLookup.getLstPartition(bvBroadcastWrapperRight.value.spatialIdx, xyCoord, k).length))
        .max
    else
      spatialIdxOther
        .allPoints
        .map(_.map(point => {

          val lookupXY = bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x * gridOpOther.squareDim, point.y * gridOpOther.squareDim)

          SpatialIdxRangeLookup.getLstPartition(spatialIdxRight, lookupXY, k).length
        }).max).max

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>LeftDS numRounds time in %,d MS, numRounds: %d".format(System.currentTimeMillis - startTime, numRounds))

    var rddPoint = rddLeft
      .mapPartitions(iter => iter.map(point => {

        var lstPartitionId = SpatialIdxRangeLookup.getLstPartition(bvBroadcastWrapperRight.value.spatialIdx, bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y), k)

        //        if (point.userData.toString.equalsIgnoreCase("taxi_2_a_827408"))
        //          println(SpatialIdxRangeLookup.getLstPartition(bvGlobalIdxRight.value, gridOperationOther.computeSquareXY(point.x, point.y), k))

        while (lstPartitionId.length < numRounds)
          lstPartitionId += -1

        lstPartitionId = Random.shuffle(lstPartitionId)

        val rDataPoint: Any = new RowData(point, SortedList[Point](k), lstPartitionId.tail)

        (lstPartitionId.head, rDataPoint)
      }))

    (0 until numRounds).foreach(_ => {
      rddPoint = rddSpIdx
        .union(new ShuffledRDD(rddPoint, rddSpIdx.partitioner.get))
        .mapPartitions(iter => {

          // first entry is always the spatial index
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

              val nextPartIdx = rowData.lstPartitionId.headOption.getOrElse(-1)

              if (rowData.lstPartitionId.nonEmpty)
                rowData.lstPartitionId = rowData.lstPartitionId.tail

              (nextPartIdx, rowData)
          })
        })

      bvBroadcastWrapperRight.unpersist()
    })

    rddPoint.map(_._2 match {
      case rowData: RowData => (rowData.point, rowData.sortedList.map(nd => (nd.distance, nd.data)))
    })
  }

  private def computeCapacity(rddPoint: RDD[Point], isAllKnn: Boolean): (Int, Long, (Double, Double, Double, Double)) = {

    var startTime = System.currentTimeMillis

    val (maxRowSize, rowCount, mbrLeft, mbrBottom, mbrRight, mbrTop) =
      rddPoint
        .mapPartitions(iter => {

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

    val rowCountAdjusted = if (isAllKnn) rowCount * 2 else rowCount

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>Right DS info time in %,d MS".format(System.currentTimeMillis - startTime))
    Helper.loggerSLf4J(debugMode, SparkKNN, ">>maxRowSize:%d\trowCount:%d\tmbrLeft:%.8f\tmbrBottom:%.8f\tmbrRight:%.8f\tmbrTop:%.8f".format(maxRowSize, rowCountAdjusted, mbrLeft, mbrBottom, mbrRight, mbrTop))

    startTime = System.currentTimeMillis

    val execCores = rddPoint.context.getConf.getOption("spark.cores.max").getOrElse("*") match {
      case "*" => (Runtime.getRuntime().availableProcessors() / rddPoint.context.getExecutorMemoryStatus.size) - 1
      case count => count.toInt
    }

    val execAvailableMemory = Helper.toByte(rddPoint.context.getConf.get("spark.executor.memory", rddPoint.context.getExecutorMemoryStatus.map(_._2._1).min + "B")) // skips memory of core assigned for Hadoop daemon
    //    val exeOverheadMemory = (0.1 * execAvailableMemory).max(384).toLong + 1 // 10% reduction in memory to account for yarn overhead
    val coresPerExec = rddPoint.context.getConf.getInt("spark.executor.cores", execCores)
    val coreAvailableMemory = execAvailableMemory / coresPerExec //  (execAvailableMemory - exeOverheadMemory - exeOverheadMemory) / coresPerExec

    val rectDummy = Rectangle(new Geom2D(0, 0), new Geom2D(0, 0))
    val pointDummy = new Point(0, 0, Array.fill[Char](maxRowSize)(' ').mkString(""))
    val sortSetDummy = SortedList[Point](k)
    val lstPartitionIdDummy = ListBuffer.fill[Int](rddPoint.getNumPartitions)(0)
    val rowDataDummy = new RowData(pointDummy, sortSetDummy, lstPartitionIdDummy)
    val spatialIndexDummy = SpatialIndex(spatialIndexType)

    val pointCost = SizeEstimator.estimate(pointDummy)
    val rowDataCost = SizeEstimator.estimate(rowDataDummy) + (pointCost * k)
    val spatialIndexCost = SizeEstimator.estimate(spatialIndexDummy) + SizeEstimator.estimate(rectDummy)

    var partObjCapacity = (coreAvailableMemory - spatialIndexCost - rowDataCost) / pointCost

    if (partObjCapacity > Int.MaxValue)
      partObjCapacity = Int.MaxValue

    val numParts = if (rowCountAdjusted < partObjCapacity)
      rddPoint.getNumPartitions
    else
      rowCountAdjusted / partObjCapacity + 1 //, (rddRight.context.getExecutorMemoryStatus.size - 1) * coresPerExec)

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>execAvailableMemory=%d\tcoresPerExec=%d\tcoreAvailableMemory=%d\tpointCost=%d\trowDataCost=%d\tspatialIndexCost=%d\tpartObjCapacity=%d\tnumParts=%d".format(execAvailableMemory, coresPerExec, coreAvailableMemory, pointCost, rowDataCost, spatialIndexCost, partObjCapacity, numParts))

    partObjCapacity = (rowCountAdjusted / numParts) + 1

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>partObjCapacity time in %,d MS".format(System.currentTimeMillis - startTime))
    Helper.loggerSLf4J(debugMode, SparkKNN, ">>partObjCapacity:%d".format(partObjCapacity))

    (partObjCapacity.toInt, rowCountAdjusted, (mbrLeft, mbrBottom, mbrRight, mbrTop))
  }
}