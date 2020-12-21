package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Circle, Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.{KdTree, KdtBranchRootNode, KdtLeafNode, KdtNode}
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.sortset.{Node, SortedLinkedList}
import org.cusp.bdi.sknn.SparkKnn.INITIAL_GRID_WIDTH
import org.cusp.bdi.sknn.ds.util.{GlobalIndexPointData, SpatialIdxOperations, SupportedSpatialIndexes}
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object SparkKnn extends Serializable {

  val INITIAL_GRID_WIDTH = 100

  def getSparkKNNClasses: Array[Class[_]] =
    Array(
      Helper.getClass,
      classOf[SortedLinkedList[_]],
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
      classOf[SparkKnn],
      BroadcastWrapper.getClass,
      SparkKnn.getClass,
      SupportedSpatialIndexes.getClass,
      classOf[SparkKnn],
      classOf[RangeInfo],
      SpatialIndex.getClass,
      classOf[SpatialIndex],
      classOf[GridOperation],
      classOf[Node[_]])
}

case class SparkKnn(debugMode: Boolean, spatialIndexType: SupportedSpatialIndexes.Value, rddLeft: RDD[Point], rddRight: RDD[Point], k: Int) extends Serializable {

  def knnJoin(): RDD[(Point, Iterable[(Double, Point)])] = {

    val (coreObjCapacityRight, spatialIndexObjCount, mbrRight) = computeCapacity(rddRight)

    val gridOpRight = new GridOperation(mbrRight, spatialIndexObjCount, k)

    knnJoinExecute(rddLeft, null, null,
      rddRight,
      mbrRight,
      SupportedSpatialIndexes(spatialIndexType),
      gridOpRight, coreObjCapacityRight)
  }

  def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] = {

    val (coreObjCapacityRight, spatialIndexObjCountRight, mbrRight) = computeCapacity(rddRight)
    val (coreObjCapacityLeft, spatialIndexObjCountLeft, mbrLeft) = computeCapacity(rddLeft)

    val coreObjCapacity = Helper.min(coreObjCapacityRight, coreObjCapacityLeft) / 2
    //    val spatialIndexObjCount = Helper.max(spatialIndexObjCountRight, spatialIndexObjCountLeft)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>All kNN adjusted coreObjCapacity=%,d".format(coreObjCapacity))

    val globalIndexRight = SupportedSpatialIndexes(spatialIndexType)
    val (gridOpRight, gridOpLeft) = (new GridOperation(mbrRight, spatialIndexObjCountRight, k), new GridOperation(mbrLeft, spatialIndexObjCountLeft, k))

    knnJoinExecute(rddLeft, null, null,
      rddRight,
      mbrRight,
      globalIndexRight,
      gridOpRight,
      coreObjCapacity)
      .union(knnJoinExecute(rddRight, globalIndexRight, gridOpRight,
        rddLeft,
        mbrLeft,
        SupportedSpatialIndexes(spatialIndexType),
        gridOpLeft,
        coreObjCapacity))
  }

  //  private def buildRangeInfo(rddForIndexing: RDD[Point], gridOpRight: GridOperation, coreObjCapacity: Long) = {
  //
  //    val stackRangeInfo = mutable.Stack[RangeInfo]()
  //
  //    // build range info
  //    rddForIndexing
  //      .mapPartitions(iter => iter.map(point => {
  //
  //        //        if (point.userData.toString.equalsIgnoreCase("Bread_1_A_165989"))
  //        //          println(">>" + gridOpRight.computeSquareXY(point.x, point.y))
  //
  //        (gridOpRight.computeSquareXY(point.x, point.y), 1L)
  //      })) // grid assignment
  //      .reduceByKey(_ + _) // summarize
  //      .sortByKey()
  //      .toLocalIterator
  //      .foreach(row => // group cells on partitions
  //
  //        if (stackRangeInfo.isEmpty || stackRangeInfo.top.totalWeight + row._2 > coreObjCapacity)
  //          stackRangeInfo.push(new RangeInfo(row))
  //        else {
  //
  //          stackRangeInfo.top.lstMBRCoord += row
  //          stackRangeInfo.top.totalWeight += row._2
  //          stackRangeInfo.top.right = row._1._1
  //
  //          //          if (row._1._2 > 12500)
  //          //            println(row)
  //
  //          if (row._1._2 < stackRangeInfo.top.bottom)
  //            stackRangeInfo.top.bottom = row._1._2
  //          else if (row._1._2 > stackRangeInfo.top.top)
  //            stackRangeInfo.top.top = row._1._2
  //        }
  //      )
  //
  //    stackRangeInfo
  //  }

  private def knnJoinExecute(rddActiveLeft: RDD[Point], globalIndexLeft: SpatialIndex, gridOpLeft: GridOperation, rddActiveRight: RDD[Point], mbrRight: (Double, Double, Double, Double), globalIndexRight: SpatialIndex, gridOpRight: GridOperation, coreObjCapacity: Long): RDD[(Point, Iterable[(Double, Point)])] = {

    var startTime = System.currentTimeMillis

    var stackRangeInfo: mutable.Stack[RangeInfo] = null // buildRangeInfo(rddActiveRight, gridOpRight, coreObjCapacity)

    // objects' density check. adjust grid's square width if any of the rangeInfo contain more than coreObjCapacity
    val stackRangeInfoOver = stackRangeInfo.filter(_.totalWeight > coreObjCapacity)

    if (stackRangeInfoOver.nonEmpty) {

      val maxRangeInfo = stackRangeInfoOver.maxBy(_.totalWeight)

      val rate = coreObjCapacity.toDouble / maxRangeInfo.totalWeight

      if (rate < 0.9) {

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>Recomputing range info. Max Total Weight: %,d Rate: %,.4f".format(maxRangeInfo.totalWeight, rate))

        gridOpRight.squareDim = math.ceil(gridOpRight.squareDim * rate).toInt

        //        stackRangeInfo = buildRangeInfo(rddActiveRight, gridOpRight, coreObjCapacity)
      }
    }

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>rangeInfo time in %,d MS. Actual number of partitions: %,d".format(System.currentTimeMillis - startTime, stackRangeInfo.length))

    startTime = System.currentTimeMillis

    // create global index
    //    var partCounter = -1
    //    globalIndexRight.insert(buildRectBounds(gridOpRight.computeSquareXY(mbrRight._1, mbrRight._2), gridOpRight.computeSquareXY(mbrRight._3, mbrRight._4)),
    //      stackRangeInfo.map(rangeInfo => {
    //
    //        partCounter += 1
    //
    //        Helper.loggerSLf4J(debugMode, SparkKnn, ">>%s\t%,d".format(rangeInfo.toString, partCounter))
    //
    //        rangeInfo
    //          .lstMBRCoord
    //          .map(row => new Point(row._1._1, row._1._2, new GlobalIndexPointData(row._2, partCounter)))
    //      })
    //        .flatMap(_.seq)
    //        .view
    //        .iterator, 1)
    //
    //    val bvBroadcastWrapperRight = rddActiveRight.context.broadcast(BroadcastWrapper(globalIndexRight, gridOpRight, stackRangeInfo.map(_.mbr).toArray))
    //
    //    Helper.loggerSLf4J(debugMode, SparkKnn, ">>GlobalIndex insert time in %,d MS. Grid size: (%,d X %,d)\tIndex: %s\tBroadcast Total Size: %,d".format(System.currentTimeMillis - startTime, bvBroadcastWrapperRight.value.gridOp.squareDim, bvBroadcastWrapperRight.value.gridOp.squareDim, bvBroadcastWrapperRight.value.spatialIdx, SizeEstimator.estimate(bvBroadcastWrapperRight.value)))
    //
    //    //    val startTime = System.currentTimeMillis
    //
    //    // build a spatial index on each partition
    //    val rddSpIdx = rddActiveRight
    //      .mapPartitions(_.map(point => (bvBroadcastWrapperRight.value.spatialIdx.findExact(bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y)).userData match {
    //        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
    //      }, point)))
    //      .partitionBy(new Partitioner() { // group points
    //
    //        override def numPartitions: Int = bvBroadcastWrapperRight.value.arrPartitionMBRs.length
    //
    //        override def getPartition(key: Any): Int = key match { // this partitioner is used when partitioning rddPoint
    //          case pIdx: Int => if (pIdx == -1) Random.nextInt(numPartitions) else pIdx
    //        }
    //      })
    //      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index
    //
    //        val startTime = System.currentTimeMillis
    //        val mbr = bvBroadcastWrapperRight.value.arrPartitionMBRs(pIdx)
    //
    //        val minX = mbr._1 * bvBroadcastWrapperRight.value.gridOp.squareDim + bvBroadcastWrapperRight.value.gridOp.datasetMBR._1 - 1e-4
    //        val minY = mbr._2 * bvBroadcastWrapperRight.value.gridOp.squareDim + bvBroadcastWrapperRight.value.gridOp.datasetMBR._2 - 1e-4
    //        val maxX = mbr._3 * bvBroadcastWrapperRight.value.gridOp.squareDim + bvBroadcastWrapperRight.value.gridOp.datasetMBR._1 + bvBroadcastWrapperRight.value.gridOp.squareDim
    //        val maxY = mbr._4 * bvBroadcastWrapperRight.value.gridOp.squareDim + bvBroadcastWrapperRight.value.gridOp.datasetMBR._2 + bvBroadcastWrapperRight.value.gridOp.squareDim
    //
    //        val rectBounds = buildRectBounds((minX, minY), (maxX, maxY))
    //
    //        val spatialIndex = SupportedSpatialIndexes(spatialIndexType)
    //
    //        spatialIndex.insert(rectBounds, iter.map(row => {
    //
    //          //          if (row._2.userData.toString.equalsIgnoreCase("Yellow_2_A_507601"))
    //          //            println(">>" + pIdx)
    //
    //          row._2
    //        }), Helper.max(bvBroadcastWrapperRight.value.gridOp.squareDim, bvBroadcastWrapperRight.value.gridOp.squareDim))
    //
    //        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %,d time in %,d MS. Index: %s. Total Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, SizeEstimator.estimate(spatialIndex)))
    //
    //        Iterator((pIdx, spatialIndex.asInstanceOf[Any]))
    //      }
    //        , preservesPartitioning = true)
    //      .persist(StorageLevel.MEMORY_ONLY)
    //
    //    //    val startTime = System.currentTimeMillis
    //
    //    val numRounds = if (globalIndexLeft == null)
    //      rddActiveLeft
    //        .mapPartitions(_.map(point => bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y)))
    //        .distinct
    //        .mapPartitions(_.map(xyCoord => SpatialIdxOperations.extractLstPartition(bvBroadcastWrapperRight.value.spatialIdx, xyCoord, k).length))
    //        .max
    //    else {
    //
    //      val shiftByX = gridOpLeft.datasetMBR._1 + gridOpLeft.squareDim - 1
    //      val shiftByY = gridOpLeft.datasetMBR._2 + gridOpLeft.squareDim - 1
    //
    //      globalIndexLeft
    //        .allPoints
    //        .map(_.map(point => {
    //
    //          //          if (point.x.toInt == 2407 && point.y.toInt == 405)
    //          //            println
    //
    //          val lookupXY = bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x * gridOpLeft.squareDim + shiftByX, point.y * gridOpLeft.squareDim + shiftByY)
    //
    //          SpatialIdxOperations.extractLstPartition(globalIndexRight, lookupXY, k).length
    //        }).max).max
    //    }
    //
    //    Helper.loggerSLf4J(debugMode, SparkKnn, ">>LeftDS numRounds time in %,d MS, numRounds: %, d".format(System.currentTimeMillis - startTime, numRounds))
    //
    //    var rddPoint = rddActiveLeft
    //      .mapPartitions(iter => iter.map(point => {
    //
    //        //        if(point.userData.toString.equalsIgnoreCase("yellow_1_a_379213"))
    //        //          println
    //
    //        val lstPartitionId = SpatialIdxOperations.extractLstPartition(bvBroadcastWrapperRight.value.spatialIdx, bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y), k)
    //
    //        //        if (lstPartitionId.length > 5)
    //        //          SpatialIdxOperations.extractLstPartition(bvBroadcastWrapperRight.value.spatialIdx, bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y), k)
    //
    //        //                if (point.userData.toString.equalsIgnoreCase("bread_2_a_979315"))
    //        //                  println(SpatialIdxOperations.extractLstPartition(bvBroadcastWrapperRight.value.spatialIdx, bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y), k))
    //
    //        //                if (point.userData.toString.equalsIgnoreCase("bread_1_b_993412"))
    //        //                  println(SpatialIdxOperations.extractLstPartition(bvBroadcastWrapperRight.value.spatialIdx, bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y), k))
    //
    //        // randomize list but maintain order
    //        while (lstPartitionId.length < numRounds)
    //          lstPartitionId.insert(Random.nextInt(lstPartitionId.length), -1)
    //
    //        //println("<<\t" + lstPartitionId.mkString("\t"))
    //        val rDataPoint: Any = new RowData(point, SortedLinkedList[Point](k), lstPartitionId.tail)
    //
    //        (lstPartitionId.head, rDataPoint)
    //      }))
    //
    //    (0 until numRounds).foreach(roundNum => {
    //      rddPoint = rddSpIdx
    //        .union(new ShuffledRDD(rddPoint, rddSpIdx.partitioner.get))
    //        .mapPartitionsWithIndex((pIdx, iter) => {
    //
    //          // first entry is always the spatial index
    //          val spatialIndex = iter.next._2 match {
    //            case spIdx: SpatialIndex => spIdx
    //          }
    //
    //          var startTime = -1L
    //          var counterRow = 0L
    //          var counterKnn = 0L
    //
    //          iter.map(row => {
    //
    //            counterRow += 1
    //
    //            if (startTime == -1)
    //              startTime = System.currentTimeMillis()
    //
    //            row._2 match {
    //              case rowData: RowData =>
    //
    //                if (row._1 != -1) {
    //
    //                  //                  if (rowData.point.userData.toString.equalsIgnoreCase("bus_3_a_589399"))
    //                  //                    println(pIdx)
    //
    //                  counterKnn += 1
    //                  spatialIndex.nearestNeighbor(rowData.point, rowData.sortedList)
    //                }
    //
    //                val nextPartIdx = rowData.lstPartitionId.headOption.getOrElse(-1)
    //
    //                if (rowData.lstPartitionId.nonEmpty)
    //                  rowData.lstPartitionId = rowData.lstPartitionId.tail
    //
    //                if (!iter.hasNext)
    //                  Helper.loggerSLf4J(debugMode, SparkKnn, ">>kNN done pIdx: %,d round: %,d points: %,d lookups: %,d in %,d MS".format(pIdx, roundNum, counterRow, counterKnn, System.currentTimeMillis() - startTime))
    //
    //                (nextPartIdx, rowData)
    //            }
    //          })
    //        })
    //
    //      bvBroadcastWrapperRight.unpersist()
    //    })
    //
    //    rddPoint.map(_._2 match {
    //      case rowData: RowData => (rowData.point, rowData.sortedList.map(nd => (nd.distance, nd.data)))
    //    })
    null
  }

  private def computeCapacity(rddPoint: RDD[Point] /*, otherSetRowCount: Long*/) = {

    var startTime = System.currentTimeMillis

    val (lstGridCells, (maxRowSize, mbrLeft, mbrBottom, mbrRight, mbrTop)) = rddPoint
      .mapPartitions(_.map(point => (math.floor(point.x).floor.toFloat, (1L, point.userData.toString.length, point.x, point.y, point.x, point.y))))
      .reduceByKey((tbl1, tbl2) =>
        (tbl1._1 + tbl2._1, Helper.max(tbl1._2, tbl2._2), Helper.min(tbl1._3, tbl2._3), Helper.min(tbl1._4, tbl2._4), Helper.max(tbl1._5, tbl2._5), Helper.max(tbl1._6, tbl2._6))
      )
      .sortByKey()
      .mapPartitions(_.map(row => (ListBuffer((row._1, row._2._1)), (row._2._2, row._2._3, row._2._4, row._2._5, row._2._6))))
      .reduce((tpl1, tpl2) =>
        (tpl1._1 ++= tpl2._1, (Helper.max(tpl1._2._1, tpl2._2._1), Helper.min(tpl1._2._2, tpl2._2._2), Helper.min(tpl1._2._3, tpl2._2._3), Helper.max(tpl1._2._4, tpl2._2._4), Helper.max(tpl1._2._5, tpl2._2._5)))
      )

    lstGridCells.foreach(row => printf(">>>\t%,.2f\t%,d%n", row._1, row._2))

    val rowCount = lstGridCells.map(_._2).sum

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS info time in %,d MS".format(System.currentTimeMillis - startTime))
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>maxRowSize:%,d\trowCount:%,d\tmbrLeft:%.8f\tmbrBottom:%.8f\tmbrRight:%.8f\tmbrTop:%.8f".format(maxRowSize, rowCount, mbrLeft, mbrBottom, mbrRight, mbrTop))

    startTime = System.currentTimeMillis

    //    val driverMemory = Helper.toByte(rddPoint.context.getConf.get("spark.driver.memory"))
    val coresPerExecutor = rddPoint.context.getConf.get("spark.executor.cores").toInt
    val execAvailableMemory = Helper.toByte(rddPoint.context.getConf.get("spark.executor.memory"))

    val execOverheadMemory = Helper.max(384, 0.1 * execAvailableMemory).toLong // 10% reduction in memory to account for yarn overhead
    //    val coreAvailableMemory = (execAvailableMemory - execOverheadMemory) / coresPerExecutor

    val rectDummy = Rectangle(new Geom2D(0, 0), new Geom2D(0, 0))
    val pointDummy = new Point(0, 0, Array.fill[Char](maxRowSize)(' ').mkString(""))
    val sortSetDummy = SortedLinkedList[Point](k)
    val lstPartitionIdDummy = ListBuffer.fill[Int](rddPoint.getNumPartitions)(0)
    val rowDataDummy = new RowData(pointDummy, sortSetDummy, lstPartitionIdDummy)
    val spatialIndexDummy = SupportedSpatialIndexes(spatialIndexType)
    val pointCost = SizeEstimator.estimate(pointDummy)
    val rowDataCost = SizeEstimator.estimate(rowDataDummy) + (pointCost * k)
    val spatialIndexCost = SizeEstimator.estimate(spatialIndexDummy) +
      SizeEstimator.estimate(rectDummy) +
      (spatialIndexDummy.estimateNodeCount(rowCount) * SizeEstimator.estimate(spatialIndexDummy.dummyNode))

    //    val execObjCapacity = (execAvailableMemory - execOverheadMemory - spatialIndexCost - rowDataCost) / pointCost
    //    var coreObjCapacity = execObjCapacity / coresPerExecutor

    var coreObjCapacity = (((execAvailableMemory - execOverheadMemory) / coresPerExecutor) - spatialIndexCost - rowDataCost) / pointCost

    //    val spatialIndexObjCount = math.min(rowCount, math.ceil(((driverMemory * 0.1) - spatialIndexCost) / pointCost).toLong)

    //    if (coreObjCapacity > Int.MaxValue)
    //      coreObjCapacity = Int.MaxValue

    //    val totalNumberOfCore s = numExecutors * coresPerExecutor

    var numParts = math.ceil(rowCount / coreObjCapacity)

    if (numParts < /*numExecutors **/ coresPerExecutor)
      numParts = /*numExecutors **/ coresPerExecutor

    coreObjCapacity = math.ceil(rowCount / numParts).toLong

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>coreObjCapacity time in %,d MS".format(System.currentTimeMillis - startTime))
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>coresPerExecutor=%,d execAvailableMemory=%,d execOverheadMemory=%,d coreObjCapacity=%,d numParts=%,.1f pointCost=%,d rowDataCost=%,d spatialIndexCost=%,d"
      .format(coresPerExecutor, execAvailableMemory, execOverheadMemory, coreObjCapacity, numParts, pointCost, rowDataCost, spatialIndexCost))

    // build partition's MBRs

    val stackRangeInfo = mutable.Stack[RangeInfo]()
    val iter = lstGridCells.iterator
    var row: (Float, Long) = null

    while (iter.hasNext) {

      if (row == null)
        row = iter.next

      var nextCell = row

      val currWeight = if (stackRangeInfo.isEmpty) 0 else stackRangeInfo.top.totalWeight

      if (currWeight + nextCell._2 > coreObjCapacity) {

        val need = coreObjCapacity - currWeight
        val newX = nextCell._1 + (need / nextCell._2.toFloat)

        nextCell = (newX, need)
        row = (newX, row._2 - need)
      }
      else
        row = null

      if (stackRangeInfo.isEmpty || currWeight == coreObjCapacity)
        stackRangeInfo.push(new RangeInfo(nextCell))
      else {

        stackRangeInfo.top.totalWeight += nextCell._2

        stackRangeInfo.top.right = nextCell._1
      }
    }

    stackRangeInfo.foreach(rInfo => println(">>>" + rInfo.toString))

    (coreObjCapacity, rowCount, (mbrLeft, mbrBottom, mbrRight, mbrTop))
  }
}