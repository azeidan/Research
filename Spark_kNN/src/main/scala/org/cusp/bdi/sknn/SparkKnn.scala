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
import org.cusp.bdi.sknn.SparkKnn.{INITIAL_GRID_WIDTH, SHUFFLE_PARTITION_MAX_BYTE_SIZE}
import org.cusp.bdi.sknn.ds.util.{GlobalIndexPointUserData, SpatialIdxOperations, SupportedSpatialIndexes}
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object SparkKnn extends Serializable {

  val INITIAL_GRID_WIDTH = 1e2
  val SHUFFLE_PARTITION_MAX_BYTE_SIZE: Double = 2e9
  //  val EXECUTOR_MEM_CACHE_RATE: Double = 0.50
  val COST_INT: Long = 4

  //  def firstIdxGT(arrCounts: Array[Int], prevMinVal: Int): Int = {
  //
  //    var minIdx = 0
  //
  //    for (i <- arrCounts.indices) {
  //      if (arrCounts(i) <= prevMinVal)
  //        return i
  //      else if (arrCounts(i) < arrCounts(minIdx))
  //        minIdx = i
  //    }
  //
  //    minIdx
  //  }

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

case class SparkKnn(debugMode: Boolean, spatialIndexType: SupportedSpatialIndexes.Value, rddLeft: RDD[Point], rddRight: RDD[Point], k: Int /*, gridWidth: Int*/) extends Serializable {

  val lstDebugInfo: ListBuffer[String] = ListBuffer()

  def knnJoin(): RDD[(Point, Iterable[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoin", lstDebugInfo)

    //    val (partObjCapacityRight, mbrInfoRight, gridWidthRight) = computeCapacity(rddRight, rddLeft, isAllKnn = false)
    val (partObjCapacityRight, mbrInfoRight) = computeCapacity(rddRight, rddLeft, isAllKnn = false)

    knnJoinExecute(rddLeft, rddRight, mbrInfoRight, partObjCapacityRight)
  }

  def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] = {

    //    val (partObjCapacityRight, mbrInfoRight, gridWidthRight) = computeCapacity(rddRight, rddLeft, isAllKnn = true)
    //    val (partObjCapacityLeft, mbrInfoLeft, gridWidthLeft) = computeCapacity(rddLeft, rddRight, isAllKnn = true)

    val (partObjCapacityRight, mbrInfoRight) = computeCapacity(rddRight, rddLeft, isAllKnn = true)
    val (partObjCapacityLeft, mbrInfoLeft) = computeCapacity(rddLeft, rddRight, isAllKnn = true)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>All kNN partObjCapacityLeft=%s partObjCapacityRight=%s".format(partObjCapacityLeft, partObjCapacityRight), lstDebugInfo)

    knnJoinExecute(rddRight, rddLeft, mbrInfoLeft, partObjCapacityLeft /*, gridWidthLeft*/)
      .union(knnJoinExecute(rddLeft, rddRight, mbrInfoRight, partObjCapacityRight /*, gridWidthRight*/))
  }

  private def knnJoinExecute(rddActiveLeft: RDD[Point],
                             rddActiveRight: RDD[Point], mbrDS_ActiveRight: MBRInfo,
                             coreObjCapacity: (Long, Long) /*, gridWidthRightActive: Int*/): RDD[(Point, Iterable[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoinExecute", lstDebugInfo)

    def computeSquareXY_Coord(x: Double, y: Double): (Int, Int) = (((x - mbrDS_ActiveRight.left) / INITIAL_GRID_WIDTH).toInt, ((y - mbrDS_ActiveRight.bottom) / INITIAL_GRID_WIDTH).toInt)

    def computeSquareXY_Point(point: Point): (Int, Int) = computeSquareXY_Coord(point.x, point.y)

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
        .mapPartitions(_.map(row => {

          if (row._2 <= coreObjCapacity._1)
            Iterator(new Point(row._1._1, row._1._2, new GlobalIndexPointUserData(row._2)))
          else {

            var rowTemp = (row._1._2.toDouble, row._2)

            (0 until Math.ceil(row._2 / coreObjCapacity._1.toDouble).toInt)
              .map(_ => {

                // scale Y-coord
                val needCount = Helper.min(coreObjCapacity._1, rowTemp._2)
                // startY + (<Needed Objects> / <Curr Count> * <height>)
                val newEndY = rowTemp._1 + (needCount / rowTemp._2.toFloat)

                val ret = new Point(row._1._1, rowTemp._1, new GlobalIndexPointUserData(needCount))

                rowTemp = (newEndY + 1e-6f, rowTemp._2 - needCount)

                ret
              })
          }
        })
          .flatMap(_.seq)
        )
        .toLocalIterator
        .map(point => { // group cells on partitions

          val globalIndexPointUserData = point.userData match {
            case globalIndexPointUserData: GlobalIndexPointUserData => globalIndexPointUserData
          }

          val newWeight = totalWeight + globalIndexPointUserData.numPoints

          if (totalWeight == 0 || totalWeight > coreObjCapacity._1 || newWeight > coreObjCapacity._2) {

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

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>rangeInfo time in %,d MS.".format(System.currentTimeMillis - startTime), lstDebugInfo)

      startTime = System.currentTimeMillis

      // create global index
      val glbIdx_ActiveRight: SpatialIndex = SupportedSpatialIndexes(spatialIndexType)
      glbIdx_ActiveRight.insert(buildRectBounds(computeSquareXY_Coord(mbrDS_ActiveRight.left, mbrDS_ActiveRight.bottom), computeSquareXY_Coord(mbrDS_ActiveRight.right, mbrDS_ActiveRight.top)), iterGlobalIndexObjects, 1)

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>GlobalIndex insert time in %,d MS. Grid size: (%,.2f X %,.2f)\tIndex: %s\tIndex Size: %,d"
        .format(System.currentTimeMillis - startTime, INITIAL_GRID_WIDTH, INITIAL_GRID_WIDTH, glbIdx_ActiveRight, -1 /*SizeEstimator.estimate(glbIdx_ActiveRight)*/), lstDebugInfo)

      bvGlobalIndexRight = rddActiveRight.context.broadcast(glbIdx_ActiveRight)
      bvArrPartitionMBRs = rddActiveRight.context.broadcast(stackRangeInfo.map(_.stretch()).toArray)

      stackRangeInfo.foreach(row =>
        Helper.loggerSLf4J(debugMode, SparkKnn, ">>\t%s".format(row.toString), lstDebugInfo))
    }

    startTime = System.currentTimeMillis

    val numRounds = rddActiveLeft
      .mapPartitions(_.map(point => (computeSquareXY_Point(point), null)))
      .reduceByKey((_, _) => null)
      .mapPartitions(iter => Iterator(iter.map(row => SpatialIdxOperations.extractLstPartition(bvGlobalIndexRight.value, row._1, k).length).max))
      .max

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>LeftDS numRounds done. numRounds: %,d time in %,d MS".format(numRounds, System.currentTimeMillis - startTime), lstDebugInfo)

    val actualNumPartitions = bvArrPartitionMBRs.value.length

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Actual number of partitions: %,d".format(actualNumPartitions), lstDebugInfo)

    // build a spatial index on each partition
    val rddSpIdx = rddActiveRight
      .mapPartitions(_.map(point => {

        val gridXY = computeSquareXY_Point(point)

        (bvGlobalIndexRight.value.findExact(gridXY._1, gridXY._2).userData match {
          case globalIndexPoint: GlobalIndexPointUserData => globalIndexPoint.partitionIdx
        }, point)
      }))
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

        val minX = mbrInfo.left * INITIAL_GRID_WIDTH + mbrDS_ActiveRight.left
        val minY = mbrInfo.bottom * INITIAL_GRID_WIDTH + mbrDS_ActiveRight.bottom
        val maxX = mbrInfo.right * INITIAL_GRID_WIDTH + mbrDS_ActiveRight.right + INITIAL_GRID_WIDTH
        val maxY = mbrInfo.top * INITIAL_GRID_WIDTH + mbrDS_ActiveRight.top + INITIAL_GRID_WIDTH

        val spatialIndex = SupportedSpatialIndexes(spatialIndexType)

        spatialIndex.insert(buildRectBounds(minX, minY, maxX, maxY), iter.map(_._2), INITIAL_GRID_WIDTH.toInt)

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %,d time in %,d MS. Index: %s\tTotal Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, -1 /*SizeEstimator.estimate(spatialIndex)*/), lstDebugInfo)

        val anyRef: AnyRef = spatialIndex

        Iterator((pIdx, anyRef))
      }, preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_ONLY)

    var rddPoint: RDD[(Int, AnyRef)] = rddActiveLeft
      .mapPartitions(_.map(point => {

        var arrPartitionId = SpatialIdxOperations.extractLstPartition(bvGlobalIndexRight.value, computeSquareXY_Point(point), k)

        if (arrPartitionId.length < numRounds) {

          arrPartitionId.sizeHint(numRounds)

          // random space the list but maintain order
          while (arrPartitionId.length < numRounds)
            arrPartitionId.insert(Random.nextInt(arrPartitionId.length + 1), -1)

          // trim trailing -1s
          arrPartitionId = arrPartitionId.slice(0, arrPartitionId.lastIndexWhere(_ >= 0) + 1)
        }

        (arrPartitionId.head, new RowData(point, new SortedLinkedList[Point](k), arrPartitionId.tail))
      }))

    (1 to numRounds).foreach(currRoundNum => {

      rddPoint = (rddSpIdx ++ rddPoint.partitionBy(rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          var counter = 0L

          // first entry is always the spatial index
          var spatialIndex: SpatialIndex = iter.next._2 match {
            case spIdx: SpatialIndex => spIdx
          }

          iter.map(row =>
            row._2 match {
              case rowData: RowData =>

                counter += 1

                if (row._1 != -1)
                  spatialIndex.nearestNeighbor(rowData.point, rowData.sortedList)

                if (!iter.hasNext) {

                  spatialIndex = null // GC the index

                  Helper.loggerSLf4J(debugMode, SparkKnn, ">>kNN done index: %,d roundNum: %,d numPoints: %,d".format(pIdx, currRoundNum, counter), lstDebugInfo)
                }

                (rowData.nextPartId, rowData)
            })
        })

      if (currRoundNum == 1) {

        bvGlobalIndexRight.unpersist(true)
        bvArrPartitionMBRs.unpersist(true)
      }
    })

    rddPoint
      .mapPartitions(_.map(_._2 match {
        case rowData: RowData => (rowData.point, rowData.sortedList.map(nd => (math.sqrt(nd.distance), nd.data)))
      }))
  }

  private def computeCapacity(rddRight: RDD[Point], rddLeft: RDD[Point], isAllKnn: Boolean): ((Long, Long), MBRInfo) = {

    val numExecutors = rddRight.context.getConf.get("spark.executor.instances").toInt
    val execAssignedMem = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory"))
    val execOverheadMem = Helper.max(384e6, 0.15 * execAssignedMem).toLong // 10% reduction in memory to account for yarn overhead
    val numCoresPerExecutor = rddRight.context.getConf.get("spark.executor.cores").toInt
    val coreAvailMem = (execAssignedMem - execOverheadMem) /* * EXECUTOR_MEM_CACHE_RATE */ / numCoresPerExecutor
    val partitionAssignedMem = Helper.min(coreAvailMem, SHUFFLE_PARTITION_MAX_BYTE_SIZE)
    val totalAvailCores = numExecutors * numCoresPerExecutor

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>numExecutors: %,d execAssignedMem: %,d numCoresPerExecutor: %,d totalAvailCores: %,d coreAvailMem: %,d partitionAssignedMem: %,.2f"
      .format(numExecutors, execAssignedMem, numCoresPerExecutor, totalAvailCores, coreAvailMem, partitionAssignedMem), lstDebugInfo)

    var startTime = System.currentTimeMillis

    val (costMaxPointRight, mbrInfoRight, rowCountRight) = rddRight
      .mapPartitions(_.map(point => (SizeEstimator.estimate(point), new MBRInfo(point.x.toFloat, point.y.toFloat), 1L)))
      .treeReduce((row1, row2) => (Helper.max(row1._1, row2._1), row1._2.merge(row2._2), row1._3 + row2._3))

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS info done costMaxPointRight: %,d mbrInfoRight: %s rowCountRight: %,d Time: %,d MS"
      .format(costMaxPointRight, mbrInfoRight, rowCountRight, System.currentTimeMillis - startTime), lstDebugInfo)

    val costRect = SizeEstimator.estimate(Rectangle(new Geom2D(), new Geom2D()))

    /*
     * Right dataset (Spatial Indexes) size estimate. Every row in the right RDD contains:
     * 1. partition ID (int) <- insignificant here, so it's not accounted for.
     * 2. spatial index: # of nodes with objects
    */
    val spatialIndexMockRight = SupportedSpatialIndexes(spatialIndexType)
    val costSpatialIndexMockRight = SizeEstimator.estimate(spatialIndexMockRight)
    spatialIndexMockRight.insert(new Rectangle(new Geom2D(Double.MaxValue / 2)), (0 until spatialIndexMockRight.nodeCapacity).map(i => new Point(i, i)).iterator, 1)
    val costSpIdxInsertOverhead = (SizeEstimator.estimate(spatialIndexMockRight) - costSpatialIndexMockRight - costRect) / spatialIndexMockRight.nodeCapacity.toDouble

    val estimateNodeCount = spatialIndexMockRight.estimateNodeCount(rowCountRight)

    val costRightRDD = (rowCountRight * costMaxPointRight) + (estimateNodeCount * (costSpatialIndexMockRight + costRect + costSpIdxInsertOverhead))

    val numPartitionsRight = Math.ceil(costRightRDD / partitionAssignedMem).toInt

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS costSpatialIndexMockRight: %,d costSpIdxInsertOverhead: %,.2f costRect: %,d estimateNodeCount: %,d costRightRDD: %,.2f numPartitionsRight: %,d"
      .format(costSpatialIndexMockRight, costSpIdxInsertOverhead, costRect, estimateNodeCount, costRightRDD, numPartitionsRight), lstDebugInfo)

    /*
     * every row in the left RDD contains:
     *   1. partition ID (int)
     *   2. point info (RowData -> (point, SortedLinkedList, list of partitions to visit))
     *
     *   point: coordinates + userData <- from raw file
     *   SortedLinkedList: point matches from the right RDD of size up to k
     */

    startTime = System.currentTimeMillis

    val (costMaxPointLeft, rowCountLeft) = rddLeft.mapPartitions(_.map(point => (SizeEstimator.estimate(point), 1L)))
      .treeReduce((row1, row2) => (Helper.max(row1._1, row2._1), row1._2 + row2._2))

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Left DS info done costMaxPointLeft: %,d rowCountLeft: %,d Time: %,d MS"
      .format(costMaxPointLeft, rowCountLeft, System.currentTimeMillis - startTime), lstDebugInfo)

    val arrPartIdMockLeft = ArrayBuffer.fill[Int](Helper.max(numPartitionsRight, totalAvailCores))(0)
    val costArrPartIdLeft = SizeEstimator.estimate(arrPartIdMockLeft)

    val sortLstMockLeft = new SortedLinkedList[Point](k) // at the end of the process, the SortedLinkedList contains k points from the right DS
    var costSortLstLeft = SizeEstimator.estimate(sortLstMockLeft)

    sortLstMockLeft.add(0, Point())
    val costSortLstInsertOverhead = SizeEstimator.estimate(sortLstMockLeft) - SizeEstimator.estimate(Point()) - costSortLstLeft + costMaxPointRight
    costSortLstLeft += k * costSortLstInsertOverhead

    val tupleMockLeft = (0, new RowData())
    val costTupleObjLeft = SizeEstimator.estimate(tupleMockLeft) + costMaxPointLeft + costArrPartIdLeft + costSortLstLeft

    val costLeftRDD = rowCountLeft * costTupleObjLeft
    val numPartitionsLeft = Math.ceil(costLeftRDD / partitionAssignedMem)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Left DS costMaxPointLeft: %,d costSortLstLeft: %,d costSortLstInsertOverhead: %,d costTupleObjLeft: %,d costLeftRDD: %,d numPartitionsLeft: %,.2f"
      .format(costMaxPointLeft, costSortLstLeft, costSortLstInsertOverhead, costTupleObjLeft, costLeftRDD, numPartitionsLeft), lstDebugInfo)

    val numPartitionsMin = Helper.max(numPartitionsRight, numPartitionsLeft).toInt
    val numPartitionsPreferred = Math.ceil(numPartitionsMin / totalAvailCores.toDouble) * totalAvailCores

    val capMin = rowCountRight / numPartitionsPreferred
    val capMax = rowCountRight / numPartitionsMin

    val coreObjCapacityRight = ((rowCountRight / numPartitionsPreferred).toLong, if (capMax / capMin > 1.1) (capMin * 1.1).toLong else capMax)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>costLeftRDD: %,d numPartitionsMin: %,d numPartitionsPreferred: %,.2f coreObjCapacityRight: %s"
      .format(costLeftRDD, numPartitionsMin, numPartitionsPreferred, coreObjCapacityRight), lstDebugInfo)

    (coreObjCapacityRight, mbrInfoRight.stretch())
  }
}