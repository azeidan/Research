package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Circle, Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.{KdTree, KdtBranchRootNode, KdtLeafNode, KdtNode}
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.sortset.{Node, SortedLinkedList}
import org.cusp.bdi.sknn.SparkKnn.{COST_INT, EXECUTOR_MEM_CACHE_RATE, INITIAL_GRID_WIDTH, SHUFFLE_PARTITION_MAX_BYTE_SIZE}
import org.cusp.bdi.sknn.ds.util.{GlobalIndexPointUserData, SpatialIdxOperations, SupportedSpatialIndexes}
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object SparkKnn extends Serializable {

  val INITIAL_GRID_WIDTH: Double = 1e2
  //  val OBJECT_COUNT_DISCOUNT_RATE: Double = 0.10
  val SHUFFLE_PARTITION_MAX_BYTE_SIZE: Double = 1e9
  val EXECUTOR_MEM_CACHE_RATE: Double = 0.50
  val COST_INT: Long = 4

  def getSparkKNNClasses: Array[Class[_]] =
    Array(
      Helper.getClass,
      classOf[SortedLinkedList[_]],
      classOf[Rectangle],
      classOf[Circle],
      classOf[Point],
      classOf[MBRInfo],
      classOf[Geom2D],
      classOf[GlobalIndexPointUserData],
      classOf[KdTree],
      classOf[QuadTree],
      KdTree.getClass,
      classOf[KdtNode],
      classOf[KdtBranchRootNode],
      classOf[KdtLeafNode],
      classOf[SparkKnn],
      SparkKnn.getClass,
      SupportedSpatialIndexes.getClass,
      SpatialIndex.getClass,
      classOf[SpatialIndex],
      classOf[Node[_]],
      classOf[ListBuffer[_]])
}

case class SparkKnn(debugMode: Boolean, spatialIndexType: SupportedSpatialIndexes.Value, rddLeft: RDD[Point], rddRight: RDD[Point], k: Int) extends Serializable {

  val lstDebugInfo: ListBuffer[String] = ListBuffer()

  def knnJoin(): RDD[(Point, Iterator[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoin", lstDebugInfo)

    val (partObjCapacityRight, squareDimRight, mbrInfoDS_R) = computeCapacity(rddRight, rddLeft, isAllKnn = false)

    knnJoinExecute(rddLeft, rddRight, mbrInfoDS_R, SupportedSpatialIndexes(spatialIndexType), squareDimRight, partObjCapacityRight)
  }

  def allKnnJoin(): RDD[(Point, Iterator[(Double, Point)])] = {

    val (partObjCapacityRight, squareDimRight, mbrInfoDS_R) = computeCapacity(rddRight, rddLeft, isAllKnn = true)
    val (partObjCapacityLeft, squareDimLeft, mbrInfoDS_L) = computeCapacity(rddLeft, rddRight, isAllKnn = true)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>All kNN partObjCapacityLeft=%s partObjCapacityRight=%s".format(partObjCapacityLeft, partObjCapacityRight), lstDebugInfo)

    knnJoinExecute(rddRight, rddLeft, mbrInfoDS_L, SupportedSpatialIndexes(spatialIndexType), squareDimLeft, partObjCapacityLeft)
      .union(knnJoinExecute(rddLeft, rddRight, mbrInfoDS_R, SupportedSpatialIndexes(spatialIndexType), squareDimRight, partObjCapacityRight))
  }

  private def knnJoinExecute(rddActiveLeft: RDD[Point],
                             rddActiveRight: RDD[Point], mbrDS_ActiveRight: MBRInfo, glbIdx_ActiveRight: SpatialIndex, gridSquareDim_ActiveRight: Int,
                             partObjCapacity: Long): RDD[(Point, Iterator[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoinExecute", lstDebugInfo)

    def computeSquareXY_Point(point: Point): (Double, Double) = (((point.x - mbrDS_ActiveRight.left) / gridSquareDim_ActiveRight).floor, ((point.y - mbrDS_ActiveRight.bottom) / gridSquareDim_ActiveRight).floor)

    def computeSquareXY_Coord(x: Double, y: Double): (Double, Double) = (((x - mbrDS_ActiveRight.left) / gridSquareDim_ActiveRight).floor, ((y - mbrDS_ActiveRight.bottom) / gridSquareDim_ActiveRight).floor)

    var startTime = System.currentTimeMillis
    var bvGlobalIndexRight: Broadcast[SpatialIndex] = null
    var bvArrPartitionMBRs: Broadcast[Array[MBRInfo]] = null

    {
      val stackRangeInfo = mutable.Stack[MBRInfo]()
      var totalWeight = 0L
      var partCounter = -1

      // build range info
      val iterGlobalIndexObjects = rddActiveRight
        .mapPartitions(_.map(point => (computeSquareXY_Point(point), 1L))) // grid assignment
        .reduceByKey(_ + _) // summarize
        .sortByKey()
        .toLocalIterator
        .map(row => { // group cells on partitions

          val newWeight = totalWeight + row._2

          if (stackRangeInfo.isEmpty || totalWeight >= partObjCapacity) {

            partCounter += 1
            totalWeight = row._2
            stackRangeInfo.push(new MBRInfo(row._1._1, row._1._2))
          } else {

            totalWeight = newWeight
            stackRangeInfo.top.right = row._1._1

            if (row._1._2 < stackRangeInfo.top.bottom)
              stackRangeInfo.top.bottom = row._1._2
            else if (row._1._2 > stackRangeInfo.top.top)
              stackRangeInfo.top.top = row._1._2
          }

          new Point(row._1, new GlobalIndexPointUserData(row._2, partCounter))
        })

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>rangeInfo time in %,d MS.".format(System.currentTimeMillis - startTime), lstDebugInfo)

      startTime = System.currentTimeMillis

      // create global index
      glbIdx_ActiveRight.insert(buildRectBounds(computeSquareXY_Coord(mbrDS_ActiveRight.left, mbrDS_ActiveRight.bottom), computeSquareXY_Coord(mbrDS_ActiveRight.right, mbrDS_ActiveRight.top)), iterGlobalIndexObjects, 1)

      bvGlobalIndexRight = rddActiveRight.context.broadcast(glbIdx_ActiveRight)
      bvArrPartitionMBRs = rddActiveRight.context.broadcast(stackRangeInfo.map(_.stretch()).toArray)

      stackRangeInfo.foreach(row =>
        Helper.loggerSLf4J(debugMode, SparkKnn, ">>\t%s".format(row.toString), lstDebugInfo))

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>GlobalIndex insert time in %,d MS. Grid size: (%,d X %,d)\tIndex: %s\tIndex Size: %,d".format(System.currentTimeMillis - startTime, gridSquareDim_ActiveRight, gridSquareDim_ActiveRight, bvGlobalIndexRight.value, SizeEstimator.estimate(bvGlobalIndexRight.value)), lstDebugInfo)
    }

    val actualNumPartitions = bvArrPartitionMBRs.value.length

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Actual number of partitions: %,d".format(actualNumPartitions), lstDebugInfo)

    // build a spatial index on each partition
    val rddSpIdx = rddActiveRight
      .mapPartitions(_.map(point => (bvGlobalIndexRight.value.findExact(computeSquareXY_Point(point)).userData match {
        case globalIndexPoint: GlobalIndexPointUserData => globalIndexPoint.partitionIdx
      }, point)))
      .partitionBy(new Partitioner() {

        override def numPartitions: Int = actualNumPartitions

        override def getPartition(key: Any): Int =
          key match {
            case pIdx: Int => if (pIdx == -1) Random.nextInt(numPartitions) else pIdx
          }
      })
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        val startTime = System.currentTimeMillis
        val mbrInfo = bvArrPartitionMBRs.value(actualNumPartitions - pIdx - 1)

        val minX = mbrInfo.left * gridSquareDim_ActiveRight + mbrDS_ActiveRight.left
        val minY = mbrInfo.bottom * gridSquareDim_ActiveRight + mbrDS_ActiveRight.bottom
        val maxX = mbrInfo.right * gridSquareDim_ActiveRight + mbrDS_ActiveRight.right + gridSquareDim_ActiveRight
        val maxY = mbrInfo.top * gridSquareDim_ActiveRight + mbrDS_ActiveRight.top + gridSquareDim_ActiveRight

        val spatialIndex = SupportedSpatialIndexes(spatialIndexType)

        spatialIndex.insert(buildRectBounds(minX, minY, maxX, maxY), iter.map(_._2), Helper.min(gridSquareDim_ActiveRight, gridSquareDim_ActiveRight))

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %,d time in %,d MS. Index: %s\tTotal Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, SizeEstimator.estimate(spatialIndex)), lstDebugInfo)

        Iterator((pIdx, spatialIndex.asInstanceOf[Any]))
      }, preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_ONLY)

    startTime = System.currentTimeMillis

    val numRounds = rddActiveLeft
      .mapPartitions(_.map(point => (computeSquareXY_Point(point), null)))
      .reduceByKey((_, _) => null)
      .mapPartitions(_.map(row => SpatialIdxOperations.extractLstPartition(bvGlobalIndexRight.value, row._1, k).length))
      .max

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>LeftDS numRounds done. numRounds: %,d time in %,d MS".format(numRounds, System.currentTimeMillis - startTime), lstDebugInfo)

    var rddPoint: RDD[(Int, Any)] = rddActiveLeft
      .mapPartitions(_.map(point => {

        val lstPartitionId = SpatialIdxOperations.extractLstPartition(bvGlobalIndexRight.value, computeSquareXY_Point(point), k)

        // randomize list, maintain order, and don't occupy the last partition.
        // only the points that require the last round will perform kNN on the last partition
        if (lstPartitionId.length < numRounds - 1)
          while (lstPartitionId.length < numRounds - 1)
            lstPartitionId.insert(Random.nextInt(lstPartitionId.length + 1), -1)

        (lstPartitionId.head, new RowData(point, new SortedLinkedList[Point](k), lstPartitionId.tail))
      }))

    /* **************************** */
    //    numRounds = 1
    /* **************************** */

    (0 until numRounds).foreach(roundNum => {

      rddPoint = (rddSpIdx ++ rddPoint.partitionBy(rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          // first entry is always the spatial index
          val spatialIndex: SpatialIndex = iter.next._2 match {
            case spIdx: SpatialIndex =>

              Helper.loggerSLf4J(debugMode, SparkKnn, ">>Got index %d roundNum: %d".format(pIdx, roundNum), lstDebugInfo)
              spIdx
          }

          iter.map(row =>
            row._2 match {
              case rowData: RowData =>

                if (row._1 != -1)
                  spatialIndex.nearestNeighbor(rowData.point, rowData.sortedList)

                if (!iter.hasNext)
                  Helper.loggerSLf4J(debugMode, SparkKnn, ">>kNN done index: %,d roundNum: %,d".format(pIdx, roundNum), lstDebugInfo)

                (rowData.nextPartId, rowData)
            })
        })

      bvGlobalIndexRight.unpersist(false)
      bvArrPartitionMBRs.unpersist(false)
    })

    rddPoint
      .mapPartitions(_.map(_._2 match {
        case rowData: RowData => (rowData.point, rowData.sortedList.iterator.map(nd => (math.sqrt(nd.distance), nd.data)))
      }))
  }

  private def computeCapacity(rddRight: RDD[Point], rddLeft: RDD[Point], isAllKnn: Boolean): (Long, Int, MBRInfo) = {

    val fPartitionInfoLeftRDD = (iter: Iterator[Point]) =>
      iter.map(point => (SizeEstimator.estimate(point), point, 1L))
        .reduce((row1, row2) =>
          if (row1._1 > row2._1)
            (row1._1, row1._2, row1._3 + row2._3)
          else
            (row2._1, row2._2, row1._3 + row2._3)
        )

    val driverAssignedMem = Helper.toByte(rddRight.context.getConf.get("spark.driver.memory"))
    val driverOverheadMem = Helper.max(384, 0.1 * driverAssignedMem).toLong // 10% reduction in memory to account for yarn overhead
    val numExecutors = rddRight.context.getConf.get("spark.executor.instances").toInt
    val execAssignedMem = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory"))
    val execOverheadMem = Helper.max(384, 0.1 * execAssignedMem).toLong // 10% reduction in memory to account for yarn overhead
    val numCoresPerExecutor = rddRight.context.getConf.get("spark.executor.cores").toInt
    val totalAvailCores = numExecutors * numCoresPerExecutor - 1

    var startTime = System.currentTimeMillis

    val (_, maxPointRight, mbrInfo, rowCountRight, gridCellCountRight) = rddRight
      .mapPartitions(_.map(point => ((math.floor(point.x / INITIAL_GRID_WIDTH), math.floor(point.y / INITIAL_GRID_WIDTH)), (SizeEstimator.estimate(point), point, new MBRInfo(point.x.toFloat, point.y.toFloat), 1L))))
      .reduceByKey((row1, row2) =>
        if (row1._1 > row2._1)
          (row1._1, row1._2, row1._3.merge(row2._3), row1._4 + row2._4)
        else
          (row2._1, row2._2, row1._3.merge(row2._3), row1._4 + row2._4)
      )
      .mapPartitions(_.map(row => (row._2._1, row._2._2, row._2._3, row._2._4, 1L)))
      .reduce((row1, row2) =>
        if (row1._1 > row2._1)
          (row1._1, row1._2, row1._3.merge(row2._3), row1._4 + row2._4, row1._5 + row2._5)
        else
          (row2._1, row2._2, row1._3.merge(row2._3), row1._4 + row2._4, row1._5 + row2._5)
      )

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS info done. maxPointRight: %s mbrInfo: %s rowCountRight: %,d gridCellCountRight: %,d Time: %,d MS"
      .format(maxPointRight.toString(), mbrInfo, rowCountRight, gridCellCountRight, System.currentTimeMillis - startTime), lstDebugInfo)

    startTime = System.currentTimeMillis

    val (_, maxPointLeft, rowCountLeft) = rddLeft.context.runJob(rddLeft, fPartitionInfoLeftRDD(_), Array(0)).head

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Left DS info done. maxPointLeft:%s rowCountLeft:%,d rddLeft.partitions.length:%,d Time:%,d MS"
      .format(maxPointLeft, rowCountLeft, rddLeft.partitions.length, System.currentTimeMillis - startTime), lstDebugInfo)

    // mock objects for right RDD and Global Index
    val rectMock = Rectangle(new Geom2D(Double.MaxValue / 2), new Geom2D(Double.MaxValue / 2))
    val costRect = SizeEstimator.estimate(rectMock)

    /*
     * Right dataset (Spatial Indexes) size estimate. Every row in the right RDD contains:
     * 1. partition ID (int)
     * 2. spatial index: # of nodes with objects
    */
    val spatialIndexMockRight = SupportedSpatialIndexes(spatialIndexType)

    val costSpIdxRight = SizeEstimator.estimate(spatialIndexMockRight) // empty SI size
    spatialIndexMockRight.insert(rectMock, Iterator(maxPointRight), 1)

    val costObjRight = SizeEstimator.estimate(spatialIndexMockRight) - costSpIdxRight - costRect

    val sizeRightRDD = rowCountRight * costObjRight + spatialIndexMockRight.estimateNodeCount(rowCountRight) * (costSpIdxRight + costRect)

    val coreAvailCacheMem = (numExecutors * (execAssignedMem - execOverheadMem) * EXECUTOR_MEM_CACHE_RATE) / (totalAvailCores + 1)
    val partitionMemSizeRight = Helper.min(coreAvailCacheMem, SHUFFLE_PARTITION_MAX_BYTE_SIZE)

    val numPartitionsRight = math.ceil(sizeRightRDD / partitionMemSizeRight)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS costSpIdxRight:%,d costObjRight:%,d sizeRightRDD:%,d coreAvailCacheMem:%,.2f partitionMemSizeRight:%,.2f numPartitionsRight:%,.2f"
      .format(costSpIdxRight, costObjRight, sizeRightRDD, coreAvailCacheMem, partitionMemSizeRight, numPartitionsRight), lstDebugInfo)

    /*
     * every row in the left RDD contains:
     *   1. partition ID (int)
     *   2. point info (RowData -> (point, SortedLinkedList, list of partitions to visit))
     *
     *   point: coordinates + userData <- from raw file
     *   SortedLinkedList: point matches from the right RDD of size up to k
     */
    val lstPartIdMockLeft = ListBuffer[Int]() // empty at the end of the process
    val sortLstMockLeft = new SortedLinkedList[Point](k)

    val costSortLst = SizeEstimator.estimate(sortLstMockLeft)
    sortLstMockLeft.add(0, maxPointRight)
    val costSortLstPoint = SizeEstimator.estimate(sortLstMockLeft) - costSortLst
    val rowDataMockLeft = new RowData(maxPointLeft, sortLstMockLeft, lstPartIdMockLeft)

    val costRowDataLeft = SizeEstimator.estimate(rowDataMockLeft) - SizeEstimator.estimate(sortLstMockLeft) + costSortLst + k * costSortLstPoint

    val sizeLeftRDD = rowCountLeft * (costRowDataLeft + COST_INT) * rddLeft.partitions.length // <- the estimate on the left RDD was for 1 partition, hence the * rddLeft.partitions.length
    val numPartitionsLeft = Helper.max(totalAvailCores, math.ceil(sizeLeftRDD / SHUFFLE_PARTITION_MAX_BYTE_SIZE))

    val numPartitions = Helper.max(numPartitionsRight, numPartitionsLeft)

    val coreObjCapacityRight = rowCountRight / numPartitions

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Left DS costRowDataLeft:%,d sizeLeftRDD:%,d numPartitionsLeft:%,.2f numPartitions:%,.2f coreObjCapacityRight:%,.2f"
      .format(costRowDataLeft, sizeLeftRDD, numPartitionsLeft, numPartitions, coreObjCapacityRight), lstDebugInfo)

    //    val coreObjCapacityMin = partitionMaxByteSize / objCostRightRDD
    //    //    var numPartitionsRight = Helper.max(numPartitionsLeft, math.ceil((if (isAllKnn) rowCountRight * 2 else rowCountRight) / coreObjCapacityMax.toDouble).toInt)
    //    if (numPartitionsRight < totalAvailCores) {
    //      numPartitionsRight = if (numExecutors == 1) totalAvailCores else numExecutors * numCoresPerExecutor - 1 // -1 for yarn
    //      coreObjCapacityMin = (if (isAllKnn) rowCountRight * 2 else rowCountRight) / numPartitionsRight
    //      if (isAllKnn)
    //        coreObjCapacityMin /= 2
    //    }
    //    if (isAllKnn)
    //      coreObjCapacityMax /= 2
    // compute global index grid box dimension
    // 100÷(284603÷(739966×.05
    //    rowCountRight = 221715342
    //    driverAssignedMem = 8e9.toLong
    //    numPartitionsRight = 245
    //    driverOverheadMem = Helper.max(384, 0.1 * driverAssignedMem).toLong
    //    objCostRightRDD = 304
    //    gridCellCountRight = 97041
    //    mbrInfo.left = 914077.4375
    //    mbrInfo.bottom = 122589.3594
    //    mbrInfo.right = 1066993.8750
    //    mbrInfo.top = 279474.4375

    val pointMockGlbIdx = new Point(0, 0, new GlobalIndexPointUserData())
    val spatialIndexMockGlbIdx = SupportedSpatialIndexes(spatialIndexType)

    //    val targetGridCellCount = math.ceil(rowCountRight * OBJECT_COUNT_DISCOUNT_RATE) // maximum packing of %10 of the number of objects

    val costSpIdxGlbIdx = SizeEstimator.estimate(spatialIndexMockGlbIdx) // empty SI size
    spatialIndexMockGlbIdx.insert(rectMock, Iterator(pointMockGlbIdx), 1)

    val costObjGlbIdx = SizeEstimator.estimate(spatialIndexMockGlbIdx) - costSpIdxGlbIdx - costRect

    val driverAvailMem = Helper.min(SHUFFLE_PARTITION_MAX_BYTE_SIZE / 2, driverAssignedMem - driverOverheadMem - (numPartitions * SizeEstimator.estimate(new MBRInfo())).toLong)
    val glbIdxNodeCount = spatialIndexMockGlbIdx.estimateNodeCount(gridCellCountRight)
    val glbIdxMemCost = glbIdxNodeCount * (costSpIdxGlbIdx + costRect) + (costObjGlbIdx * gridCellCountRight) // 2 * to account for memory for sorting
    //    val glbIdxMemCost_10_per = spatialIndexMockGlbIdx.estimateNodeCount((rowCountRight * 0.10).toLong) * (costSpIdxGlbIdx + costRect) + (costObjGlbIdx * rowCountRight * 0.10) // 2 * to account for memory for sorting
    val glbIdxAvailMem = Helper.min(driverAvailMem, glbIdxMemCost)
    var adjustRate = glbIdxMemCost / glbIdxAvailMem

    //    if (adjustRate < 0) adjustRate = 1 + adjustRate

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Global Index costSpIdxGlbIdx:%,d costObjGlbIdx:%,d driverAvailMem: %,.2f glbIdxNodeCount: %,d glbIdxMemCost: %,d glbIdxAvailMem: %,.2f adjustRate: %,.2f"
      .format(costSpIdxGlbIdx, costObjGlbIdx, driverAvailMem, glbIdxNodeCount, glbIdxMemCost, glbIdxAvailMem, adjustRate), null)

    //    if (glbIdxMemCost > maxCostGlbIdxMem) {
    //
    //      adjustRate = adjustRate / (glbIdxMemCost / maxCostGlbIdxMem)
    //
    //      Helper.loggerSLf4J(debugMode, SparkKnn, ">>(glbIdxMemCost > maxCostGlbIdxMem) adjustRate: %,.8f".format(adjustRate), null)
    //    }

    val glbIdxGridSquareDim = math.ceil(INITIAL_GRID_WIDTH / adjustRate).toInt

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>glbIdxGridSquareDim:%,d".format(glbIdxGridSquareDim), lstDebugInfo)
    //        System.exit(-555)
    (coreObjCapacityRight.toLong, glbIdxGridSquareDim.toInt, mbrInfo.stretch())
  }
}