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
import org.cusp.bdi.sknn.SparkKnn.{SHUFFLE_PARTITION_MAX_BYTE_SIZE, expandAndBuildRectBounds}
import org.cusp.bdi.sknn.ds.util.SpatialIdxOperations.fCastToGlobalIndexPointUserData
import org.cusp.bdi.sknn.ds.util.{GlobalIndexPointUserData, SpatialIdxOperations, SupportedSpatialIndexes}
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object SparkKnn extends Serializable {

  val SHUFFLE_PARTITION_MAX_BYTE_SIZE: Double = 2e9

  def expandAndBuildRectBounds(mbrInfo: MBRInfo, gridWidth: Int): Rectangle =
    buildRectBounds(mbrInfo.left * gridWidth - gridWidth,
      mbrInfo.bottom * gridWidth - gridWidth,
      mbrInfo.right * gridWidth + gridWidth,
      mbrInfo.top * gridWidth + gridWidth)

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
      classOf[mutable.Stack[MBRInfo]],
      classOf[RandomWeighted2])
}

case class SparkKnn(debugMode: Boolean, spatialIndexType: SupportedSpatialIndexes.Value, rddLeft: RDD[Point], rddRight: RDD[Point], k: Int, initialGridWidth: Int) extends Serializable {

  val lstDebugInfo: ListBuffer[String] = ListBuffer()

  def knnJoin(): RDD[(Point, Iterable[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoin", lstDebugInfo)

    val (partObjCapacityRight, mbrInfoRight, gridWidthRight) = computeCapacity(rddRight, rddLeft, isAllKnn = false)

    knnJoinExecute(rddLeft, rddRight, mbrInfoRight, partObjCapacityRight, gridWidthRight)
  }

  def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] = {

    val (partObjCapacityRight, mbrInfoRight, gridWidthRight) = computeCapacity(rddRight, rddLeft, isAllKnn = true)
    val (partObjCapacityLeft, mbrInfoLeft, gridWidthLeft) = computeCapacity(rddLeft, rddRight, isAllKnn = true)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>All kNN partObjCapacityLeft=%s partObjCapacityRight=%s".format(partObjCapacityLeft, partObjCapacityRight), lstDebugInfo)

    knnJoinExecute(rddRight, rddLeft, mbrInfoLeft, partObjCapacityLeft, gridWidthLeft)
      .union(knnJoinExecute(rddLeft, rddRight, mbrInfoRight, partObjCapacityRight, gridWidthRight))
  }

  private def knnJoinExecute(rddActiveLeft: RDD[Point], rddActiveRight: RDD[Point], mbrDS_ActiveRight: MBRInfo, coreObjCapacity: (Long, Long), gridWidthRightActive: Int): RDD[(Point, Iterable[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoinExecute", lstDebugInfo)

    def computeSquareXY_Point(point: Point): (Int, Int) = computeSquareXY_Coord(point.x, point.y)

    def computeSquareXY_Coord(x: Double, y: Double): (Int, Int) = (Helper.round(x / gridWidthRightActive), Helper.round(y / gridWidthRightActive))

    var startTime = System.currentTimeMillis
    var bvArrPartitionMBR_ActiveRight: Broadcast[Array[MBRInfo]] = null
    var bvGlobalIndex_ActiveRight: Broadcast[SpatialIndex] = null

    {
      val stackRangeInfo = mutable.Stack[MBRInfo]()
      var currObjCountMBR = 0L
      var partCounter = -1

      // build range info
      val iterGlobalIndexObjects = rddActiveRight
        .mapPartitions(_.map(point => (computeSquareXY_Point(point), 1L))) // grid assignment
        .reduceByKey(_ + _) // summarize
        .sortByKey() // sorts by (x, y)
        .collect()
        .toStream
        .map(row => { // group cells on partitions

          val newObjCountMBR = currObjCountMBR + row._2

          if (currObjCountMBR == 0 || currObjCountMBR > coreObjCapacity._1 || newObjCountMBR > coreObjCapacity._2) {

            partCounter += 1
            currObjCountMBR = row._2
            stackRangeInfo.push(new MBRInfo(row._1))
          }
          else {

            currObjCountMBR = newObjCountMBR
            stackRangeInfo.top.update(row._1)
          }

          new Point(row._1._1, row._1._2, new GlobalIndexPointUserData(row._2, partCounter))
        })
        .iterator

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>rangeInfo time in %,d MS.".format(System.currentTimeMillis - startTime), lstDebugInfo)

      startTime = System.currentTimeMillis

      // create global index
      val glbIdx_ActiveRight = SupportedSpatialIndexes(spatialIndexType)
      glbIdx_ActiveRight.insert(buildRectBounds(computeSquareXY_Coord(mbrDS_ActiveRight.left, mbrDS_ActiveRight.bottom), computeSquareXY_Coord(mbrDS_ActiveRight.right, mbrDS_ActiveRight.top)), iterGlobalIndexObjects, 1)

      bvGlobalIndex_ActiveRight = rddActiveRight.context.broadcast(glbIdx_ActiveRight)

      bvArrPartitionMBR_ActiveRight = rddActiveRight.context.broadcast(stackRangeInfo.toArray)

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>GlobalIndex insert time in %,d MS. Grid size: (%,d X %,d)\tIndex: %s\tIndex Size: %,d".format(System.currentTimeMillis - startTime, gridWidthRightActive, gridWidthRightActive, glbIdx_ActiveRight, -1 /*SizeEstimator.estimate(glbIdx_ActiveRight)*/), lstDebugInfo)

      stackRangeInfo.foreach(row =>
        Helper.loggerSLf4J(debugMode, SparkKnn, ">>\t%s".format(row.toString), lstDebugInfo))
    }

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Actual number of partitions: %,d".format(bvArrPartitionMBR_ActiveRight.value.length), lstDebugInfo)

    // build a spatial index on each partition
    val rddSpIdx: RDD[(Int, AnyRef)] = rddActiveRight
      .mapPartitions(_.map(point => {
        // the (actualNumPartitions - 1 - arr index) due to the stack. Points are added in reverse order!
        val gridXY = computeSquareXY_Point(point)

        (fCastToGlobalIndexPointUserData(bvGlobalIndex_ActiveRight.value.findExact(gridXY._1, gridXY._2)).partitionIdx, point)
      }))
      .partitionBy(new Partitioner() {

        override def numPartitions: Int = bvArrPartitionMBR_ActiveRight.value.length

        override def getPartition(key: Any): Int =
          key match {
            case pIdx: Int => if (pIdx < 0) -pIdx - 1 else pIdx
          }
      })
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        val startTime = System.currentTimeMillis
        // The (actualNumPartitions - 1 - arr index) due to the stack. Points are added in reverse order!
        val mbrInfo = bvArrPartitionMBR_ActiveRight.value(bvArrPartitionMBR_ActiveRight.value.length - 1 - pIdx)

        val spatialIndex = SupportedSpatialIndexes(spatialIndexType)

        spatialIndex.insert(expandAndBuildRectBounds(mbrInfo, gridWidthRightActive), iter.map(_._2), gridWidthRightActive)

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %,d time in %,d MS. Index: %s\tTotal Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, -1 /*SizeEstimator.estimate(spatialIndex)*/), lstDebugInfo)

        Iterator((pIdx, spatialIndex.asInstanceOf[AnyRef]))
      }, preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_AND_DISK)

    startTime = System.currentTimeMillis

    val numRounds = rddActiveLeft
      .mapPartitions(_.map(point => (computeSquareXY_Point(point), null)))
      .reduceByKey((_, _) => null) // distinct
      .mapPartitions(_.map(row => SpatialIdxOperations.extractLstPartition(bvGlobalIndex_ActiveRight.value, row._1, k).length))
      .max

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>LeftDS numRounds done. numRounds: %,d time in %,d MS".format(numRounds, System.currentTimeMillis - startTime), lstDebugInfo)

    var rddPoint: RDD[(Int, AnyRef)] = rddActiveLeft
      .mapPartitions(_.map(point => {

        val arrPartitionId = SpatialIdxOperations.extractLstPartition(bvGlobalIndex_ActiveRight.value, computeSquareXY_Point(point), k)

        if (arrPartitionId.length < numRounds) {

          val setSelectedPartitions = arrPartitionId.to[mutable.Set]

          arrPartitionId.sizeHint(numRounds)

          while (arrPartitionId.length < numRounds) {

            var rand = Random.nextInt(bvArrPartitionMBR_ActiveRight.value.length)

            while (!setSelectedPartitions.add(rand))
              rand = Random.nextInt(bvArrPartitionMBR_ActiveRight.value.length)

            arrPartitionId.insert(Random.nextInt(arrPartitionId.length + 1), -(rand + 1))
          }
        }
        //        if(point.userData.toString.equalsIgnoreCase("bread_1_a_599155"))
        //          println
        (arrPartitionId.head, new RowData(point, new SortedLinkedList[Point](k), arrPartitionId.tail))
      }))

    (1 to numRounds).foreach(currRoundNum =>
      rddPoint = (rddSpIdx ++ rddPoint.partitionBy(rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          var counter = 0L

          // first entry is always the spatial index
          val spatialIndex: SpatialIndex = iter.next._2 match {
            case spIdx: SpatialIndex => spIdx
          }

          iter.map(row =>
            row._2 match {
              case rowData: RowData =>

                counter += 1

                if (row._1 >= 0) {

                  //                  if(rowData.point.userData.toString.equalsIgnoreCase("bread_1_a_599155"))
                  //                    println

                  spatialIndex.nearestNeighbor(rowData.point, rowData.sortedList)
                }

                if (!iter.hasNext)
                  Helper.loggerSLf4J(debugMode, SparkKnn, ">>kNN done index: %,d roundNum: %,d numPoints: %,d".format(pIdx, currRoundNum, counter), lstDebugInfo)

                (rowData.nextPartId(pIdx), rowData)
            })
        }))

    rddPoint
      .mapPartitions(_.map(_._2 match {
        case rowData: RowData => (rowData.point, rowData.sortedList.map(nd => (Math.sqrt(nd.distance), nd.data)))
      }))
  }

  private def computeCapacity(rddRight: RDD[Point], rddLeft: RDD[Point], isAllKnn: Boolean): ((Long, Long), MBRInfo, Int /*, Int*/ ) = {

    val numExecutors = rddRight.context.getConf.get("spark.executor.instances").toInt
    val execAssignedMem = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory"))
    val execOverheadMem = Helper.max(384e6, 0.15 * execAssignedMem).toLong // 15% reduction in memory to account for yarn overhead
    val numCoresPerExecutor = rddRight.context.getConf.get("spark.executor.cores").toInt
    val coreAvailMem = (execAssignedMem - execOverheadMem) / numCoresPerExecutor
    val partitionAssignedMem = Helper.min(coreAvailMem, SHUFFLE_PARTITION_MAX_BYTE_SIZE)
    val totalAvailCores = numExecutors * numCoresPerExecutor

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>numExecutors: %,d execAssignedMem: %,d numCoresPerExecutor: %,d totalAvailCores: %,d coreAvailMem: %,d partitionAssignedMem: %,.2f"
      .format(numExecutors, execAssignedMem, numCoresPerExecutor, totalAvailCores, coreAvailMem, partitionAssignedMem), lstDebugInfo)

    var startTime = System.currentTimeMillis

    val (costMaxPointRight, mbrInfoRight, maxGridCellCount, rowCountRight) = rddRight
      .mapPartitions(_.map(point => ((Math.floor(point.x / initialGridWidth).toInt, Math.floor(point.y / initialGridWidth).toInt), (SizeEstimator.estimate(point), new MBRInfo(point.x.toInt, point.y.toInt), 1L))))
      .reduceByKey((row1, row2) => (Helper.max(row1._1, row2._1), row1._2.merge(row2._2), row1._3 + row2._3))
      .mapPartitions(_.map(row => (row._2._1, row._2._2, row._2._3, row._2._3)))
      .treeReduce((row1, row2) => (Helper.max(row1._1, row2._1), row1._2.merge(row2._2), Helper.max(row1._3, row2._3), row1._4 + row2._4)
      )

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS info done costMaxPointRight: %,d mbrInfoRight: %s maxGridCellCount: %,d rowCountRight: %,d Time: %,d MS"
      .format(costMaxPointRight, mbrInfoRight, maxGridCellCount, rowCountRight, System.currentTimeMillis - startTime), lstDebugInfo)

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

    val costRightRDD = (rowCountRight * costMaxPointRight) + (estimateNodeCount * (costSpatialIndexMockRight + costRect + costSpIdxInsertOverhead)) * (if (isAllKnn) 2 else 1)

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

    val costLeftRDD = rowCountLeft * costTupleObjLeft * (if (isAllKnn) 2 else 1)
    val numPartitionsLeft = Math.ceil(costLeftRDD / partitionAssignedMem)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Left DS costMaxPointLeft: %,d costSortLstLeft: %,d costSortLstInsertOverhead: %,d costTupleObjLeft: %,d costLeftRDD: %,d numPartitionsLeft: %,.2f"
      .format(costMaxPointLeft, costSortLstLeft, costSortLstInsertOverhead, costTupleObjLeft, costLeftRDD, numPartitionsLeft), lstDebugInfo)

    val numPartitionsMin = Helper.max(numPartitionsRight, numPartitionsLeft).toInt
    val numPartitionsPreferred = (Math.ceil(numPartitionsMin / totalAvailCores.toDouble) * totalAvailCores).toLong

    val coreObjCapacityRight = (rowCountRight / numPartitionsPreferred, rowCountRight / numPartitionsMin)

    val rate = 2.0 * maxGridCellCount / coreObjCapacityRight._1
    val gridWidth = (if (rate > 0.50) // maxGridCellCount has more than 1/2 coreObjCapacity
      initialGridWidth / Math.ceil(Math.sqrt(rate))
    else
      initialGridWidth
      ).toInt

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>costLeftRDD: %,d numPartitionsMin: %,d numPartitionsPreferred: %,d coreObjCapacityRight: %s rate: %,.4f gridWidth: %,d"
      .format(costLeftRDD, numPartitionsMin, numPartitionsPreferred, coreObjCapacityRight, rate, gridWidth), lstDebugInfo)

    (coreObjCapacityRight, mbrInfoRight, gridWidth /*, totalAvailCores*/ )
  }
}