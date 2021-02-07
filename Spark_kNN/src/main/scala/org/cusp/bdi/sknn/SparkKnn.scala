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
import org.cusp.bdi.sknn.SparkKnn.{EXECUTOR_MEM_CACHE_RATE, SHUFFLE_PARTITION_MAX_BYTE_SIZE}
import org.cusp.bdi.sknn.ds.util.{GlobalIndexPointUserData, SpatialIdxOperations, SupportedSpatialIndexes}
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object SparkKnn extends Serializable {

  val SHUFFLE_PARTITION_MAX_BYTE_SIZE: Double = 2e9
  val EXECUTOR_MEM_CACHE_RATE: Double = 0.50
  val COST_INT: Long = 4

  def getSparkKNNClasses: Array[Class[_]] =
    Array(
      Helper.getClass,
      SupportedKnnOperations.getClass,
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
      classOf[ListBuffer[_]],
      classOf[mutable.Stack[MBRInfo]])
}

case class SparkKnn(debugMode: Boolean, spatialIndexType: SupportedSpatialIndexes.Value, rddLeft: RDD[Point], rddRight: RDD[Point]) extends Serializable {

  val lstDebugInfo: ListBuffer[String] = ListBuffer()

  def knnJoin(k: Int, gridWidth: Int): RDD[(Point, Iterable[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoin", lstDebugInfo)

    val (partObjCapacityRight, mbrInfoDS_R) = computeCapacity(rddRight, rddLeft, isAllKnn = false, k)

    knnJoinExecute(rddLeft, rddRight, k, gridWidth, mbrInfoDS_R, SupportedSpatialIndexes(spatialIndexType), partObjCapacityRight)
  }

  def allKnnJoin(k: Int, gridWidth: Int): RDD[(Point, Iterable[(Double, Point)])] = {

    val (partObjCapacityRight, mbrInfoDS_R) = computeCapacity(rddRight, rddLeft, isAllKnn = true, k)
    val (partObjCapacityLeft, mbrInfoDS_L) = computeCapacity(rddLeft, rddRight, isAllKnn = true, k)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>All kNN partObjCapacityLeft=%s partObjCapacityRight=%s".format(partObjCapacityLeft, partObjCapacityRight), lstDebugInfo)

    knnJoinExecute(rddRight, rddLeft, k, gridWidth, mbrInfoDS_L, SupportedSpatialIndexes(spatialIndexType), partObjCapacityLeft)
      .union(knnJoinExecute(rddLeft, rddRight, k, gridWidth, mbrInfoDS_R, SupportedSpatialIndexes(spatialIndexType), partObjCapacityRight))
  }

  private def knnJoinExecute(rddActiveLeft: RDD[Point],
                             rddActiveRight: RDD[Point], k: Int, gridWidth: Int, mbrDS_ActiveRight: MBRInfo, glbIdx_ActiveRight: SpatialIndex,
                             partObjCapacity: Long): RDD[(Point, Iterable[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoinExecute", lstDebugInfo)

    def computeSquareXY_Coord(x: Double, y: Double): (Double, Double) = (((x - mbrDS_ActiveRight.left) / gridWidth).toInt, ((y - mbrDS_ActiveRight.bottom) / gridWidth).toInt)

    def computeSquareXY_Point(point: Point): (Double, Double) = computeSquareXY_Coord(point.x, point.y)


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
        .sortByKey() // sorts by (x, y)
        .mapPartitions(_.map(row => new Point(row._1, new GlobalIndexPointUserData(row._2))))
        .collect()
        .map(point => { // group cells on partitions

          val globalIndexPointUserData = point.userData match {
            case globalIndexPointUserData: GlobalIndexPointUserData => globalIndexPointUserData
          }

          val newWeight = totalWeight + globalIndexPointUserData.numPoints

          if (totalWeight == 0 || totalWeight >= partObjCapacity) {

            partCounter += 1
            totalWeight = globalIndexPointUserData.numPoints
            stackRangeInfo.push(new MBRInfo(point.x, point.y))
          }
          else {

            totalWeight = newWeight
            stackRangeInfo.top.right = point.x

            if (point.y < stackRangeInfo.top.bottom)
              stackRangeInfo.top.bottom = point.y
            else if (point.y > stackRangeInfo.top.top)
              stackRangeInfo.top.top = point.y
          }

          globalIndexPointUserData.partitionIdx = partCounter

          point
        })
        .iterator

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>rangeInfo time in %,d MS.".format(System.currentTimeMillis - startTime), lstDebugInfo)

      startTime = System.currentTimeMillis

      // create global index
      glbIdx_ActiveRight.insert(buildRectBounds(computeSquareXY_Coord(mbrDS_ActiveRight.left, mbrDS_ActiveRight.bottom), computeSquareXY_Coord(mbrDS_ActiveRight.right, mbrDS_ActiveRight.top)), iterGlobalIndexObjects, 1)

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>GlobalIndex insert time in %,d MS. Grid size: (%,d X %,d)\tIndex: %s\tIndex Size: %,d".format(System.currentTimeMillis - startTime, gridWidth, gridWidth, glbIdx_ActiveRight, -1 /*SizeEstimator.estimate(glbIdx_ActiveRight)*/), lstDebugInfo)

      bvGlobalIndexRight = rddActiveRight.context.broadcast(glbIdx_ActiveRight)
      bvArrPartitionMBRs = rddActiveRight.context.broadcast(stackRangeInfo.map(_.stretch()).toArray)

      stackRangeInfo.foreach(row =>
        Helper.loggerSLf4J(debugMode, SparkKnn, ">>\t%s".format(row.toString), lstDebugInfo))
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
            case pIdx: Int =>
              if (pIdx == -1) Random.nextInt(numPartitions) else pIdx
          }
      })
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        val startTime = System.currentTimeMillis
        val mbrInfo = bvArrPartitionMBRs.value(actualNumPartitions - pIdx - 1)

        val minX = mbrInfo.left * gridWidth + mbrDS_ActiveRight.left
        val minY = mbrInfo.bottom * gridWidth + mbrDS_ActiveRight.bottom
        val maxX = mbrInfo.right * gridWidth + mbrDS_ActiveRight.right + gridWidth
        val maxY = mbrInfo.top * gridWidth + mbrDS_ActiveRight.top + gridWidth

        val spatialIndex = SupportedSpatialIndexes(spatialIndexType)

        spatialIndex.insert(buildRectBounds(minX, minY, maxX, maxY), iter.map(_._2), gridWidth)

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %,d time in %,d MS. Index: %s\tTotal Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, -1 /*SizeEstimator.estimate(spatialIndex)*/), lstDebugInfo)

        Iterator((pIdx, spatialIndex.asInstanceOf[AnyRef]))
      }, preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_ONLY)

    startTime = System.currentTimeMillis

    val numRounds = rddActiveLeft
      .mapPartitions(_.map(point => (computeSquareXY_Point(point), null)))
      .reduceByKey((_, _) => null)
      .mapPartitions(iter => Iterator(iter.map(row => SpatialIdxOperations.extractLstPartition(bvGlobalIndexRight.value, row._1, k).length).max))
      .max

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>LeftDS numRounds done. numRounds: %,d time in %,d MS".format(numRounds, System.currentTimeMillis - startTime), lstDebugInfo)

    var rddPoint: RDD[(Int, AnyRef)] = rddActiveLeft
      .mapPartitions(_.map(point => {

        val arrPartitionId = SpatialIdxOperations.extractLstPartition(bvGlobalIndexRight.value, computeSquareXY_Point(point), k)

        arrPartitionId.sizeHint(numRounds)

        // random-space the list but maintain order
        while (arrPartitionId.length < numRounds /* -1 */ )
          arrPartitionId.insert(Random.nextInt(arrPartitionId.length + 1), -1)

        (arrPartitionId.head, new RowData(point, new SortedLinkedList[Point](k), arrPartitionId.tail))
      }))

    (0 until numRounds).foreach(roundNum => {

      rddPoint = (rddSpIdx ++ rddPoint.partitionBy(rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          // first entry is always the spatial index
          val spatialIndex: SpatialIndex = iter.next._2 match {
            case spIdx: SpatialIndex => spIdx
          }

          var counter = 0L

          iter.map(row =>
            row._2 match {
              case rowData: RowData =>

                counter += 1

                if (row._1 != -1)
                  spatialIndex.nearestNeighbor(rowData.point, rowData.sortedList)

                if (!iter.hasNext)
                  Helper.loggerSLf4J(debugMode, SparkKnn, ">>kNN done index: %,d roundNum: %,d numPoints: %,d".format(pIdx, roundNum, counter), lstDebugInfo)

                (rowData.nextPartId, rowData)
            })
        })

      bvGlobalIndexRight.unpersist(false)
      bvArrPartitionMBRs.unpersist(false)
    })

    rddPoint
      .mapPartitions(_.map(_._2 match {
        case rowData: RowData => (rowData.point, rowData.sortedList.map(nd => (math.sqrt(nd.distance), nd.data)))
      }))
  }

  private def computeCapacity(rddRight: RDD[Point], rddLeft: RDD[Point], isAllKnn: Boolean, k: Int): (Long, MBRInfo) = {

    //    val driverAssignedMem = Helper.toByte(rddRight.context.getConf.get("spark.driver.memory"))
    //    val driverOverheadMem = Helper.max(384, 0.1 * driverAssignedMem).toLong // 10% reduction in memory to account for yarn overhead
    val numExecutors = rddRight.context.getConf.get("spark.executor.instances").toInt
    val execAssignedMem = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory"))
    val execOverheadMem = Helper.max(384, 0.1 * execAssignedMem).toLong // 10% reduction in memory to account for yarn overhead
    val numCoresPerExecutor = rddRight.context.getConf.get("spark.executor.cores").toInt
    val totalAvailCores = numExecutors * numCoresPerExecutor
    //    val coreAvailMem = ((execAssignedMem - execOverheadMem) * EXECUTOR_MEM_CACHE_RATE) / numCoresPerExecutor
    val coreAvailMem = ((execAssignedMem - execOverheadMem) * EXECUTOR_MEM_CACHE_RATE) / numCoresPerExecutor
    val partitionAssignedMem = Helper.min(coreAvailMem, SHUFFLE_PARTITION_MAX_BYTE_SIZE)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>numExecutors: %,d execAssignedMem: %,d numCoresPerExecutor: %,d totalAvailCores: %,d coreAvailMem: %,.2f partitionAssignedMem: %,.2f"
      .format(numExecutors, execAssignedMem, numCoresPerExecutor, totalAvailCores, coreAvailMem, partitionAssignedMem), lstDebugInfo)

    var startTime = System.currentTimeMillis

    val (costMaxPointRight, mbrInfo, rowCountRight) = rddRight
      .mapPartitions(_.map(point => (SizeEstimator.estimate(point), new MBRInfo(point.x, point.y), 1L)))
      .treeReduce((row1, row2) =>
        (Helper.max(row1._1, row2._1), row1._2.merge(row2._2), row1._3 + row2._3))

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS info done costMaxPointRight: %,d  mbrInfo: %s rowCountRight: %,d Time: %,d MS"
      .format(costMaxPointRight, mbrInfo, rowCountRight, System.currentTimeMillis - startTime), lstDebugInfo)

    startTime = System.currentTimeMillis

    val (costMaxPointLeft, rowCountLeft) = rddLeft.mapPartitions(_.map(point => (SizeEstimator.estimate(point), 1L)))
      .treeReduce((row1, row2) =>
        (Helper.max(row1._1, row2._1), row1._2 + row2._2))

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Left DS info done costMaxPointLeft: %,d maxPointLeft: %s rddLeft.partitions.length: %,d Time: %,d MS"
      .format(costMaxPointLeft, rowCountLeft, rddLeft.partitions.length, System.currentTimeMillis - startTime), lstDebugInfo)

    val costObjRect = SizeEstimator.estimate(Rectangle(new Geom2D(), new Geom2D()))

    /*
     * Right dataset (Spatial Indexes) size estimate. Every row in the right RDD contains:
     * 1. partition ID (int) <- insignificant here, so it's not accounted for.
     * 2. spatial index: # of nodes with objects
    */

    val spatialIndexMockRight = SupportedSpatialIndexes(spatialIndexType)
    val costObjSpatialIndexMockRight = SizeEstimator.estimate(spatialIndexMockRight)
    spatialIndexMockRight.insert(new Rectangle(new Geom2D(Double.MaxValue / 2)), (0 until spatialIndexMockRight.nodeCapacity).map(i => new Point(i, i)).iterator, 1)
    val costSpIdxInsertOverhead = (SizeEstimator.estimate(spatialIndexMockRight) - costObjSpatialIndexMockRight - costObjRect) / spatialIndexMockRight.nodeCapacity.toDouble

    val costRightRDD = (rowCountRight * costMaxPointRight) + (spatialIndexMockRight.estimateNodeCount(rowCountRight) * (costObjSpatialIndexMockRight + costObjRect + costSpIdxInsertOverhead))

    val numPartitionsRight = math.ceil(costRightRDD / partitionAssignedMem)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS costObjSpatialIndexMockRight: %,d costSpIdxInsertOverhead: %,.2f costRect: %,d costRightRDD: %,.2f numPartitionsRight: %,.2f"
      .format(costObjSpatialIndexMockRight, costSpIdxInsertOverhead, costObjRect, costRightRDD, numPartitionsRight), lstDebugInfo)

    /*
     * every row in the left RDD contains:
     *   1. partition ID (int)
     *   2. point info (RowData -> (point, SortedLinkedList, list of partitions to visit))
     *
     *   point: coordinates + userData <- from raw file
     *   SortedLinkedList: point matches from the right RDD of size up to k
     */
    val arrPartIdMockLeft = ArrayBuffer.fill[Int](numPartitionsRight.toInt)(0)
    val costLstPartIdLeft = SizeEstimator.estimate(arrPartIdMockLeft)

    val sortLstMockLeft = new SortedLinkedList[Point](k) // at the end of the process, the SortedLinkedList contains k points from the right DS
    var costSortLstLeft = SizeEstimator.estimate(sortLstMockLeft)

    sortLstMockLeft.add(0, Point())
    val costSortLstInsertOverhead = SizeEstimator.estimate(sortLstMockLeft) - costSortLstLeft + costMaxPointRight
    costSortLstLeft += k * costSortLstInsertOverhead

    val tupleMockLeft = (0, new RowData())
    val costTupleObjLeft = SizeEstimator.estimate(tupleMockLeft) + costMaxPointLeft + costLstPartIdLeft + costSortLstLeft

    val costLeftRDD = rowCountLeft * costTupleObjLeft
    val numPartitionsLeft = Helper.max(math.ceil(costLeftRDD / partitionAssignedMem), totalAvailCores - 1)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Left DS costSortLstInsertOverhead: %,d costTupleObjLeft: %,d costLeftRDD: %,d numPartitionsLeft: %,.2f".format(costSortLstInsertOverhead, costTupleObjLeft, costLeftRDD, numPartitionsLeft), lstDebugInfo)

    val numPartitions = Helper.max(numPartitionsRight, numPartitionsLeft)
    val coreObjCapacityRight = rowCountRight / numPartitions
    //    val coreObjCapacityRightMax = rowCountRight / numPartitionsRight

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>costLeftRDD: %,d numPartitions: %,.2f coreObjCapacityRight: %,.2f"
      .format(costLeftRDD, numPartitions, coreObjCapacityRight), lstDebugInfo)

    //        System.exit(-555)
    (coreObjCapacityRight.toLong, mbrInfo.stretch())
  }
}