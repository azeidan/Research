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
import org.cusp.bdi.sknn.ds.util.{GlobalIndexPoint, SpatialIdxOperations, SupportedSpatialIndexes}
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object SparkKnn extends Serializable {

  //  private val ROW_SCALE_FACTOR = 0.0001
  def getSparkKNNClasses: Array[Class[_]] =
    Array(
      Helper.getClass,
      classOf[SortedLinkedList[_]],
      classOf[Rectangle],
      classOf[Circle],
      classOf[Point],
      classOf[MBRInfo],
      classOf[Geom2D],
      classOf[GlobalIndexPoint],
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

    val (partObjCapacityRight, squareDimRight, mbrInfoDS_R) = computeCapacity(rddRight, isAllKnn = false)

    knnJoinExecute(rddLeft, rddRight, mbrInfoDS_R, SupportedSpatialIndexes(spatialIndexType), squareDimRight, partObjCapacityRight)
  }

  def allKnnJoin(): RDD[(Point, Iterator[(Double, Point)])] = {

    val (partObjCapacityRight, squareDimRight, mbrInfoDS_R) = computeCapacity(rddRight, isAllKnn = true)
    val (partObjCapacityLeft, squareDimLeft, mbrInfoDS_L) = computeCapacity(rddLeft, isAllKnn = true)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>All kNN partObjCapacityLeft=%s partObjCapacityRight=%s".format(partObjCapacityLeft, partObjCapacityRight), lstDebugInfo)

    knnJoinExecute(rddRight, rddLeft, mbrInfoDS_L, SupportedSpatialIndexes(spatialIndexType), squareDimLeft, partObjCapacityLeft)
      .union(knnJoinExecute(rddLeft, rddRight, mbrInfoDS_R, SupportedSpatialIndexes(spatialIndexType), squareDimRight, partObjCapacityRight))
  }

  private def knnJoinExecute(rddActiveLeft: RDD[Point],
                             rddActiveRight: RDD[Point], mbrDS_ActiveRight: MBRInfo, gIdx_ActiveRight: SpatialIndex, gridSquareDim_ActiveRight: Int,
                             partObjCapacity: (Long, Long)): RDD[(Point, Iterator[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoinExecute", lstDebugInfo)

    def computeSquarePoint(point: Point): (Double, Double) = (((point.x - mbrDS_ActiveRight.left) / gridSquareDim_ActiveRight).floor, ((point.y - mbrDS_ActiveRight.bottom) / gridSquareDim_ActiveRight).floor)

    def computeSquareXY(x: Double, y: Double): (Double, Double) = (((x - mbrDS_ActiveRight.left) / gridSquareDim_ActiveRight).floor, ((y - mbrDS_ActiveRight.bottom) / gridSquareDim_ActiveRight).floor)

    var startTime = System.currentTimeMillis
    var bvGlobalIndexRight: Broadcast[SpatialIndex] = null
    var bvArrPartitionMBRs: Broadcast[Array[MBRInfo]] = null

    {
      val stackRangeInfo = mutable.Stack[MBRInfo]()
      var totalWeight = 0L
      var partCounter = -1

      // build range info
      val iterGlobalIndexObjects = rddActiveRight
        .mapPartitions(_.map(point => (computeSquarePoint(point), 1L))) // grid assignment
        .reduceByKey(_ + _) // summarize
        .sortByKey()
        .toLocalIterator
        .map(row => { // group cells on partitions

          val newWeight = totalWeight + row._2

          if (stackRangeInfo.isEmpty || totalWeight > partObjCapacity._1 || newWeight > partObjCapacity._2) {

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

          new Point(row._1, new GlobalIndexPoint(row._2, partCounter))
        })

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>rangeInfo time in %,d MS.".format(System.currentTimeMillis - startTime), lstDebugInfo)

      startTime = System.currentTimeMillis

      // create global index
      gIdx_ActiveRight.insert(buildRectBounds(computeSquareXY(mbrDS_ActiveRight.left, mbrDS_ActiveRight.bottom), computeSquareXY(mbrDS_ActiveRight.right, mbrDS_ActiveRight.top)), iterGlobalIndexObjects, 1)

      bvGlobalIndexRight = rddActiveRight.context.broadcast(gIdx_ActiveRight)
      bvArrPartitionMBRs = rddActiveRight.context.broadcast(stackRangeInfo.map(_.stretch()).toArray)

      stackRangeInfo.foreach(row =>
        Helper.loggerSLf4J(debugMode, SparkKnn, ">>\t%s".format(row.toString), lstDebugInfo))

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>GlobalIndex insert time in %,d MS. Grid size: (%,d X %,d)\tIndex: %s\tIndex Size: %,d".format(System.currentTimeMillis - startTime, gridSquareDim_ActiveRight, gridSquareDim_ActiveRight, bvGlobalIndexRight.value, SizeEstimator.estimate(bvGlobalIndexRight.value)), lstDebugInfo)
    }

    val actualNumPartitions = bvArrPartitionMBRs.value.length

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Actual number of partitions: %,d".format(actualNumPartitions), lstDebugInfo)

    // build a spatial index on each partition
    val rddSpIdx = rddActiveRight
      .mapPartitions(_.map(point => (bvGlobalIndexRight.value.findExact(computeSquarePoint(point)).userData match {
        case globalIndexPoint: GlobalIndexPoint => globalIndexPoint.partitionIdx
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

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %,d time in %,d MS. Index: %s. Total Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, SizeEstimator.estimate(spatialIndex)), lstDebugInfo)

        Iterator((pIdx, spatialIndex.asInstanceOf[Any]))
      }, preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_AND_DISK)

    startTime = System.currentTimeMillis

    val numRounds = rddActiveLeft
      .mapPartitions(_.map(point => (computeSquarePoint(point), null)))
      .reduceByKey((_, _) => null)
      .mapPartitions(_.map(row => SpatialIdxOperations.extractLstPartition(bvGlobalIndexRight.value, row._1, k).length))
      .max

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>LeftDS numRounds done. numRounds: %, d, time in %,d MS".format(numRounds, System.currentTimeMillis - startTime), lstDebugInfo)

    var rddPoint: RDD[(Int, Any)] = rddActiveLeft
      .mapPartitions(_.map(point => {

        //                if (point.userData.toString.equalsIgnoreCase("Yellow_1_B_142099"))
        //                  println

        val lstPartitionId = SpatialIdxOperations.extractLstPartition(bvGlobalIndexRight.value, computeSquarePoint(point), k)

        // randomize list, maintain order, and don't occupy the last partition.
        // only the points that require the last round will perform kNN on the last partition
        if (lstPartitionId.length < numRounds - 1)
          while (lstPartitionId.length < numRounds - 1)
            lstPartitionId.insert(Random.nextInt(lstPartitionId.length + 1), -1)

        (lstPartitionId.head, new RowData(point, new SortedLinkedList[Point](k), lstPartitionId.tail))
      }))

    (0 until numRounds).foreach(roundNum => {

      rddPoint = (rddSpIdx ++ rddPoint.partitionBy(rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          var startTime = System.currentTimeMillis()

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
                  Helper.loggerSLf4J(debugMode, SparkKnn, ">>kNN done index %d roundNum: %d".format(pIdx, roundNum), lstDebugInfo)

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

  private def computeCapacity(rddRight: RDD[Point], isAllKnn: Boolean): ((Long, Long), Int, MBRInfo) = {

    var startTime = System.currentTimeMillis

    val initialGridWidth = 100.0

    val (maxCount, minCount, maxRowSize, mbrInfo, rowCount, gridCellCount) = rddRight
      .mapPartitions(_.map(point => ((math.floor(point.x / initialGridWidth), math.floor(point.y / initialGridWidth)), (point.userData.toString.length, new MBRInfo(point.x.toFloat, point.y.toFloat), 1L))))
      .reduceByKey((row1, row2) => (Helper.max(row1._1, row2._1), row1._2.merge(row2._2), row1._3 + row2._3))
      .mapPartitions(_.map(row => (row._2._3, row._2._3, row._2._1, row._2._2, row._2._3, 1L)))
      .reduce((row1, row2) => (Helper.max(row1._1, row2._1), Helper.min(row1._2, row2._2), Helper.max(row1._3, row2._3), row1._4.merge(row2._4), row1._5 + row2._5, row1._6 + row2._6))

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS info time in %,d MS".format(System.currentTimeMillis - startTime), lstDebugInfo)
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>maxRowSize:%,d\trowCount:%,d\tmbr:%s".format(maxRowSize, rowCount, mbrInfo), lstDebugInfo)

    startTime = System.currentTimeMillis

    val driverAssignedMem = Helper.toByte(rddRight.context.getConf.get("spark.driver.memory"))
    val driverOverheadMem = Helper.max(384, 0.1 * driverAssignedMem).toLong // 10% reduction in memory to account for yarn overhead
    val numExecutors = rddRight.context.getConf.get("spark.executor.instances").toInt
    val execAssignedMem = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory"))
    val execOverheadMem = Helper.max(384, 0.1 * execAssignedMem).toLong // 10% reduction in memory to account for yarn overhead
    val numCoresPerExecutor = rddRight.context.getConf.get("spark.executor.cores").toInt
    val totalAvailCores = numExecutors * numCoresPerExecutor - 1

    //  Mock objects for size estimate
    val rectMock = Rectangle(new Geom2D(), new Geom2D())
    val pointMock = new Point(0, 0, ("%" + maxRowSize + "s").format(" "))
    val sortListMock = new SortedLinkedList[Point](k)
    val lstPartitionIdMock = ListBuffer.fill[Int](rddRight.partitions.length)(0)
    val rowDataMock = new RowData(pointMock, sortListMock, lstPartitionIdMock)
    val spatialIndexMock = SupportedSpatialIndexes(spatialIndexType)

    val rectCost = SizeEstimator.estimate(rectMock)
    val spatialIndexCost = SizeEstimator.estimate(spatialIndexMock) // empty SI size

    spatialIndexMock.insert(rectMock, Iterator(pointMock), 1)

    val objCost = SizeEstimator.estimate(spatialIndexMock) - spatialIndexCost - rectCost

    val sortListCost = SizeEstimator.estimate(sortListMock)
    sortListMock.add(0, pointMock)
    val rowDataCost = SizeEstimator.estimate(rowDataMock) + sortListCost + ((SizeEstimator.estimate(sortListMock) - sortListCost) * k)

    // System wide cache capability
    val systemAvailMem = numExecutors * (execAssignedMem - execOverheadMem - (rowDataCost * numCoresPerExecutor))
    val allSpatialIdxCost = spatialIndexMock.estimateNodeCount(rowCount) * (spatialIndexCost + rectCost)
    val coreAvailMem = (systemAvailMem - allSpatialIdxCost) / totalAvailCores
    var coreObjCapacityMax = coreAvailMem / objCost
    var coreObjCapacityMin = coreObjCapacityMax

    var numPartitions = math.ceil((if (isAllKnn) rowCount * 2 else rowCount) / coreObjCapacityMax.toDouble).toInt

    if (numPartitions < totalAvailCores) {

      numPartitions = if (numExecutors == 1) totalAvailCores else (numExecutors - 1) * numCoresPerExecutor

      coreObjCapacityMin = (if (isAllKnn) rowCount * 2 else rowCount) / numPartitions

      if (isAllKnn)
        coreObjCapacityMin /= 2
    }

    if (isAllKnn)
      coreObjCapacityMax /= 2

    // compute global index grid box dimension
    // 100÷(284603÷(739966×.05
    var rate = maxCount / (coreObjCapacityMin * 0.05)
    var gIdxGridSquareDim = math.ceil(initialGridWidth / rate).toInt
    var gIdxObjCount = math.ceil(gridCellCount * rate).toLong
    val gIdxNodeCount = spatialIndexMock.estimateNodeCount(gIdxObjCount)
    var gIdxMemCost = gIdxNodeCount * (spatialIndexCost + rectCost) + (objCost * gridCellCount) // 2 * to account for memory for sorting

    // adjust for global index memory on the driver
    val driverAvailMem = driverAssignedMem - driverOverheadMem

    if (driverAvailMem < gIdxMemCost) {

      rate = driverAvailMem.toDouble / gIdxMemCost

      gIdxGridSquareDim = math.ceil(gIdxGridSquareDim / rate).toInt

      //      gIdxObjCount = spatialIndexMock.estimateObjCount()
      gIdxObjCount = math.ceil(gIdxObjCount * rate).toLong

      if (gIdxObjCount > coreObjCapacityMax)
        throw InsufficientMemoryException("The assigned driver memory is insufficient for the dataset provided")
    }

    gIdxMemCost = spatialIndexMock.estimateNodeCount(gIdxObjCount) * (spatialIndexCost + rectCost) + (objCost * gridCellCount)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>numExecutors:%,d totalAvailCores:%,d driverAssignedMem:%,d execAssignedMem:%,d ,coreObjCapacityMin:%,d coreObjCapacityMax:%,d numPartitions:%,d objCost:%,d rowDataCost:%,d spatialIndexCost:%,d gIdxObjCount:%,d gIdxMemCost:%,d gIdxGridSquareDim:%,d Time in %,d MS"
      .format(numExecutors, totalAvailCores, driverAssignedMem, execAssignedMem, coreObjCapacityMin, coreObjCapacityMax, numPartitions, objCost, rowDataCost, spatialIndexCost, gIdxObjCount, gIdxMemCost, gIdxGridSquareDim, System.currentTimeMillis - startTime), lstDebugInfo)

    ((coreObjCapacityMin, coreObjCapacityMax), gIdxGridSquareDim, mbrInfo.stretch())
  }
}