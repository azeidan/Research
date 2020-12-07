package org.cusp.bdi.sknn

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
import org.cusp.bdi.sknn.ds.util.{GlobalIndexPointData, SpatialIdxOperations, SupportedSpatialIndexes}
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.Helper

import scala.collection.mutable.ListBuffer
import scala.util.Random

object SparkKnn extends Serializable {

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

    val (partObjCapacityRight, mbrRight) = computeCapacity(rddRight)

    val gridOpRight = new GridOperation(mbrRight, partObjCapacityRight, k)

    knnJoinExecute(rddLeft, null, null,
      rddRight,
      buildRectBounds(gridOpRight.computeSquareXY(mbrRight._1, mbrRight._2), gridOpRight.computeSquareXY(mbrRight._3, mbrRight._4)),
      SupportedSpatialIndexes(spatialIndexType),
      gridOpRight, partObjCapacityRight)
  }

  def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] = {

    val (partObjCapacityRight, mbrRight) = computeCapacity(rddRight)
    val (partObjCapacityLeft, mbrLeft) = computeCapacity(rddLeft)

    val partObjCapacity = Helper.min(partObjCapacityRight, partObjCapacityLeft) / 2

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>All kNN adjusted partObjCapacity=%,d".format(partObjCapacity))

    val globalIndexRight = SupportedSpatialIndexes(spatialIndexType)
    val (gridOpRight, gridOpLeft) = (new GridOperation(mbrRight, partObjCapacity, k), new GridOperation(mbrLeft, partObjCapacity, k))

    knnJoinExecute(rddLeft, null, null,
      rddRight,
      buildRectBounds(gridOpRight.computeSquareXY(mbrRight._1, mbrRight._2), gridOpRight.computeSquareXY(mbrRight._3, mbrRight._4)),
      globalIndexRight,
      gridOpRight,
      partObjCapacity)
      .union(knnJoinExecute(rddRight, globalIndexRight, gridOpRight,
        rddLeft,
        buildRectBounds(gridOpLeft.computeSquareXY(mbrLeft._1, mbrLeft._2), gridOpLeft.computeSquareXY(mbrLeft._3, mbrLeft._4)),
        SupportedSpatialIndexes(spatialIndexType),
        gridOpLeft,
        partObjCapacity))
  }

  private def knnJoinExecute(rddActiveLeft: RDD[Point], globalIndexLeft: SpatialIndex, gridOpLeft: GridOperation, rddActiveRight: RDD[Point], rectGlobalIdxRight: Rectangle, globalIndexRight: SpatialIndex, gridOpRight: GridOperation, partObjCapacity: Long): RDD[(Point, Iterable[(Double, Point)])] = {

    var startTime = System.currentTimeMillis
    var bvBroadcastWrapperRight: Broadcast[BroadcastWrapper] = null
    val lstRangeInfo = ListBuffer[RangeInfo]()
    var rangeInfo: RangeInfo = null

    // build range info
    rddActiveRight
      .mapPartitions(iter => iter.map(point => {

        //        if(point.userData.toString.equalsIgnoreCase("taxi_2_b_255747"))
        //          println(gridOpRight.computeSquareXY(point.x, point.y))

        (gridOpRight.computeSquareXY(point.x, point.y), 1L)
      })) // grid assignment
      .reduceByKey(_ + _) // summarize
      .sortByKey()
      .toLocalIterator
      .foreach(row => // group cells on partitions

        if (rangeInfo == null || rangeInfo.totalWeight + row._2 > partObjCapacity) {

          rangeInfo = new RangeInfo(row)
          lstRangeInfo += rangeInfo
        }
        else {

          rangeInfo.lstMBRCoord += row
          rangeInfo.totalWeight += row._2
          rangeInfo.right = row._1._1

          if (row._1._2 < rangeInfo.bottom)
            rangeInfo.bottom = row._1._2
          else if (row._1._2 > rangeInfo.top)
            rangeInfo.top = row._1._2
        })

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>rangeInfo time in %,d MS".format(System.currentTimeMillis - startTime))

    startTime = System.currentTimeMillis

    // create global index
    var actualPartitionCount = -1
    globalIndexRight.insert(rectGlobalIdxRight, lstRangeInfo.map(rangeInfo => {

      actualPartitionCount += 1

      rangeInfo
        .lstMBRCoord
        .map(row => new Point(row._1._1, row._1._2, new GlobalIndexPointData(row._2, actualPartitionCount)))
    })
      .flatMap(_.seq)
      .view
      .iterator, 1)

    bvBroadcastWrapperRight = rddActiveRight.context.broadcast(BroadcastWrapper(globalIndexRight, gridOpRight, lstRangeInfo.map(_.mbr).toArray))

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>GlobalIndex insert time in %,d MS. Index: %s\tBroadcast Total Size: %,d".format(System.currentTimeMillis - startTime, bvBroadcastWrapperRight.value.spatialIdx, SizeEstimator.estimate(bvBroadcastWrapperRight.value)))

    //    val startTime = System.currentTimeMillis

    // build a spatial index on each partition
    val rddSpIdx = rddActiveRight
      .mapPartitions(_.map(point => (bvBroadcastWrapperRight.value.spatialIdx.findExact(bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y)).userData match {
        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
      }, point)))
      .partitionBy(new Partitioner() { // group points

        override def numPartitions: Int = bvBroadcastWrapperRight.value.arrPartitionMBRs.length

        override def getPartition(key: Any): Int = key match { // this partitioner is used when partitioning rddPoint
          case pIdx: Int => if (pIdx == -1) Random.nextInt(numPartitions) else pIdx
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

        val spatialIndex = SupportedSpatialIndexes(spatialIndexType)

        spatialIndex.insert(rectBounds, iter.map(row => {

          //          if (row._2.userData.toString.equalsIgnoreCase("Taxi_2_A_163401"))
          //            println(">>" + pIdx)

          row._2
        }), bvBroadcastWrapperRight.value.gridOp.squareDim)

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %d time in %,d MS. Index: %s. Total Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, SizeEstimator.estimate(spatialIndex)))

        Iterator((pIdx, spatialIndex.asInstanceOf[Any]))
      }
        , preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_ONLY)

    //    val startTime = System.currentTimeMillis

    val numRounds = if (globalIndexLeft == null)
      rddActiveLeft
        .mapPartitions(_.map(point => bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y)))
        .distinct
        .mapPartitions(_.map(xyCoord => SpatialIdxOperations.extractLstPartition(bvBroadcastWrapperRight.value.spatialIdx, xyCoord, k).length))
        .max
    else {

      val shiftBy = gridOpLeft.squareDim - 1

      globalIndexLeft
        .allPoints
        .map(_.map(point => {

          //          if (point.x.toInt == 2407 && point.y.toInt == 405)
          //            println

          val lookupXY = bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x * gridOpLeft.squareDim + shiftBy, point.y * gridOpLeft.squareDim + shiftBy)

          SpatialIdxOperations.extractLstPartition(globalIndexRight, lookupXY, k).length
        }).max).max
    }

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>LeftDS numRounds time in %,d MS, numRounds: %d".format(System.currentTimeMillis - startTime, numRounds))

    var rddPoint = rddActiveLeft
      .mapPartitions(iter => iter.map(point => {

        var lstPartitionId = SpatialIdxOperations.extractLstPartition(bvBroadcastWrapperRight.value.spatialIdx, bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y), k)

        //        if (point.userData.toString.equalsIgnoreCase("taxi_2_b_255747"))
        //          println(SpatialIdxOperations.extractLstPartition(bvBroadcastWrapperRight.value.spatialIdx, bvBroadcastWrapperRight.value.gridOp.computeSquareXY(point.x, point.y), k))

        while (lstPartitionId.length < numRounds)
          lstPartitionId += -1

        lstPartitionId = Random.shuffle(lstPartitionId)

        val rDataPoint: Any = new RowData(point, SortedList[Point](k), lstPartitionId.tail)

        (lstPartitionId.head, rDataPoint)
      }))

    (0 until numRounds).foreach(_ => {
      rddPoint = rddSpIdx
        .union(new ShuffledRDD(rddPoint, rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          // first entry is always the spatial index
          val spatialIndex = iter.next._2 match {
            case spIdx: SpatialIndex => spIdx
          }

          iter.map(row => row._2 match {
            case rowData: RowData =>

              if (row._1 != -1) {

                //                if (rowData.point.userData.toString.equalsIgnoreCase("taxi_2_b_255747"))
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

  private def computeCapacity(rddPoint: RDD[Point] /*, otherSetRowCount: Long*/): (Long, (Double, Double, Double, Double)) = {

    var startTime = System.currentTimeMillis

    val (maxRowSize, rowCount, mbrLeft, mbrBottom, mbrRight, mbrTop) =
      rddPoint
        .mapPartitions(_.map(point => (point.userData.toString.length, 1L, point.x, point.y, point.x, point.y)))
        .reduce((param1, param2) =>
          (Helper.max(param1._1, param2._1), param1._2 + param2._2, Helper.min(param1._3, param2._3), Helper.min(param1._4, param2._4), Helper.max(param1._5, param2._5), Helper.max(param1._6, param2._6)))

    //    val rowCountAdjusted = if (isAllKnn) rowCount * 2 else rowCount

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS info time in %,d MS".format(System.currentTimeMillis - startTime))
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>maxRowSize:%d\trowCount:%d\tmbrLeft:%.8f\tmbrBottom:%.8f\tmbrRight:%.8f\tmbrTop:%.8f".format(maxRowSize, rowCount, mbrLeft, mbrBottom, mbrRight, mbrTop))

    startTime = System.currentTimeMillis

    //    val numExecutors = rddPoint.context.getConf.get("spark.executor.instances").toInt
    val coresPerExecutor = rddPoint.context.getConf.get("spark.executor.cores").toInt
    val execAvailableMemory = Helper.toByte(rddPoint.context.getConf.get("spark.executor.memory"))

    val execOverheadMemory = Helper.max(384, 0.1 * execAvailableMemory).toLong // 10% reduction in memory to account for yarn overhead
    //    val coreAvailableMemory = (execAvailableMemory - execOverheadMemory) / coresPerExecutor

    val rectDummy = Rectangle(new Geom2D(0, 0), new Geom2D(0, 0))
    val pointDummy = new Point(0, 0, Array.fill[Char](maxRowSize)(' ').mkString(""))
    val sortSetDummy = SortedList[Point](k)
    val lstPartitionIdDummy = ListBuffer.fill[Int](rddPoint.getNumPartitions)(0)
    val rowDataDummy = new RowData(pointDummy, sortSetDummy, lstPartitionIdDummy)
    val spatialIndexDummy = SupportedSpatialIndexes(spatialIndexType)

    //    val rowCountAdjusted = rowCount + otherSetRowCount

    val pointCost = SizeEstimator.estimate(pointDummy)
    val rowDataCost = SizeEstimator.estimate(rowDataDummy) + (pointCost * k)
    val spatialIndexCost = SizeEstimator.estimate(spatialIndexDummy) +
      SizeEstimator.estimate(rectDummy) +
      spatialIndexDummy.estimateNodeCount(rowCount) * SizeEstimator.estimate(spatialIndexDummy.dummyNode)

    val execObjCapacity = (execAvailableMemory - execOverheadMemory - spatialIndexCost - rowDataCost) / pointCost
    var coreObjCapacity = execObjCapacity / coresPerExecutor

    //    if (coreObjCapacity > Int.MaxValue)
    //      coreObjCapacity = Int.MaxValue

    //    val totalNumberOfCore s = numExecutors * coresPerExecutor

    var numParts = rowCount / coreObjCapacity + 1

    if (numParts < /*numExecutors **/ coresPerExecutor)
      numParts = /*numExecutors **/ coresPerExecutor

    coreObjCapacity = rowCount / numParts

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>coreObjCapacity time in %,d MS".format(System.currentTimeMillis - startTime))
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>coresPerExecutor=%,d, execAvailableMemory=%,d, execOverheadMemory=%,d, execObjCapacity=%,d, coreObjCapacity=%,d, numParts=%,d, pointCost=%,d, rowDataCost=%,d, spatialIndexCost=%,d"
      .format(coresPerExecutor, execAvailableMemory, execOverheadMemory, execObjCapacity, coreObjCapacity, numParts, pointCost, rowDataCost, spatialIndexCost))

    (coreObjCapacity, (mbrLeft, mbrBottom, mbrRight, mbrTop))
  }
}