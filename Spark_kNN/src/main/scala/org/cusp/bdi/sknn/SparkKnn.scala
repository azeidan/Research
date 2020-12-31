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
import org.cusp.bdi.util.Helper
import org.cusp.bdi.util.Helper.FLOAT_ERROR_RANGE

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
      classOf[KdTree],
      KdTree.getClass,
      classOf[QuadTree],
      classOf[KdTree],
      classOf[KdtNode],
      classOf[KdtBranchRootNode],
      classOf[KdtLeafNode],
      classOf[SparkKnn],
      SparkKnn.getClass,
      SupportedSpatialIndexes.getClass,
      classOf[SparkKnn],
      classOf[RangeInfo],
      SpatialIndex.getClass,
      classOf[SpatialIndex],
      classOf[Node[_]])
}

case class SparkKnn(debugMode: Boolean, spatialIndexType: SupportedSpatialIndexes.Value, rddLeft: RDD[Point], rddRight: RDD[Point], k: Int) extends Serializable {

  val lstDebugInfo: ListBuffer[String] = ListBuffer()

  def knnJoin(): RDD[(Point, Iterator[(Double, Point)])] = {

    val (partObjCapacityRight, squareDimRight, mbrDS) = computeCapacity(rddRight, isAllKnn = false)

    knnJoinExecute(rddLeft, rddRight, mbrDS, SupportedSpatialIndexes(spatialIndexType), squareDimRight, partObjCapacityRight)
  }

  def allKnnJoin(): RDD[(Point, Iterator[(Double, Point)])] = {

    val (partObjCapacityLeft, squareDimLeft, mbrDS_L) = computeCapacity(rddLeft, isAllKnn = true)
    val (partObjCapacityRight, squareDimRight, mbrDS_R) = computeCapacity(rddRight, isAllKnn = true)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>All kNN partObjCapacityLeft=%,d partObjCapacityRight=%,d".format(partObjCapacityLeft, partObjCapacityRight), lstDebugInfo)

    knnJoinExecute(rddLeft, rddRight, mbrDS_R, SupportedSpatialIndexes(spatialIndexType), squareDimRight, partObjCapacityRight)
      .union(knnJoinExecute(rddRight, rddLeft, mbrDS_L, SupportedSpatialIndexes(spatialIndexType), squareDimLeft, partObjCapacityLeft))
  }

  private def knnJoinExecute(rddActiveLeft: RDD[Point],
                             rddActiveRight: RDD[Point], mbrDS_R: (Double, Double, Double, Double), globalIndexRight: SpatialIndex, squareDimRight: Int,
                             partObjCapacity: Long): RDD[(Point, Iterator[(Double, Point)])] = {

    def computeSquareXY(x: Double, y: Double): (Double, Double) = (((x - mbrDS_R._1) / squareDimRight).floor, ((y - mbrDS_R._2) / squareDimRight).floor)

    def computeSquarePoint(point: Point): (Double, Double) = computeSquareXY(point.x, point.y)

    var startTime = System.currentTimeMillis

    val stackRangeInfo = mutable.Stack[RangeInfo]()

    // build range info
    rddActiveRight
      .mapPartitions(_.map(point => (computeSquarePoint(point), 1L))) // grid assignment
      .reduceByKey(_ + _) // summarize
      .sortByKey()
      .toLocalIterator
      .foreach(row => // group cells on partitions

        if (stackRangeInfo.isEmpty || stackRangeInfo.top.totalWeight + row._2 > partObjCapacity)
          stackRangeInfo.push(new RangeInfo(row))
        else {

          stackRangeInfo.top.lstMBRCoord += row
          stackRangeInfo.top.totalWeight += row._2
          stackRangeInfo.top.right = row._1._1

          if (row._1._2 < stackRangeInfo.top.bottom)
            stackRangeInfo.top.bottom = row._1._2
          else if (row._1._2 > stackRangeInfo.top.top)
            stackRangeInfo.top.top = row._1._2
        }
      )

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>rangeInfo time in %,d MS. Actual number of partitions: %,d".format(System.currentTimeMillis - startTime, stackRangeInfo.length), lstDebugInfo)

    startTime = System.currentTimeMillis

    // create global index
    var partCounter = -1
    globalIndexRight.insert(buildRectBounds(computeSquareXY(mbrDS_R._1, mbrDS_R._2), computeSquareXY(mbrDS_R._3, mbrDS_R._4)),
      stackRangeInfo.map(rangeInfo => {

        partCounter += 1

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>%s\t%,d".format(rangeInfo.toString, partCounter), lstDebugInfo)

        rangeInfo
          .lstMBRCoord
          .map(row => new Point(row._1, new GlobalIndexPointData(row._2, partCounter)))
      })
        .flatMap(_.seq)
        .view
        .iterator, 1)

    val bvGlobalIndexRight = rddActiveRight.context.broadcast(globalIndexRight)
    val bvArrPartitionMBRs = rddActiveRight.context.broadcast(stackRangeInfo.map(_.mbr).toArray)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>GlobalIndex insert time in %,d MS. Grid size: (%,d X %,d)\tIndex: %s\tIndex Size: %,d".format(System.currentTimeMillis - startTime, squareDimRight, squareDimRight, bvGlobalIndexRight.value, SizeEstimator.estimate(bvGlobalIndexRight.value)), lstDebugInfo)

    // build a spatial index on each partition
    val rddSpIdx = rddActiveRight.mapPartitions(_.map(point => (bvGlobalIndexRight.value.findExact(computeSquarePoint(point)).userData match {
      case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
    }, point)))
      .partitionBy(new Partitioner() { // group points

        override def numPartitions: Int = bvArrPartitionMBRs.value.length

        override def getPartition(key: Any): Int = key match { // this partitioner is used when partitioning rddPoint
          case pIdx: Int => if (pIdx == -1) Random.nextInt(numPartitions) else pIdx
        }
      })
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        val startTime = System.currentTimeMillis
        val mbr = bvArrPartitionMBRs.value(pIdx)

        val minX = mbr._1 * squareDimRight + mbrDS_R._1 //- FLOAT_ERROR_RANGE
        val minY = mbr._2 * squareDimRight + mbrDS_R._2 //- FLOAT_ERROR_RANGE
        val maxX = mbr._3 * squareDimRight + mbrDS_R._3 + squareDimRight
        val maxY = mbr._4 * squareDimRight + mbrDS_R._4 + squareDimRight

        val rectBounds = buildRectBounds((minX, minY), (maxX, maxY))

        val spatialIndex = SupportedSpatialIndexes(spatialIndexType)

        spatialIndex.insert(rectBounds, iter.map(_._2), squareDimRight)

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %,d time in %,d MS. Index: %s. Total Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, SizeEstimator.estimate(spatialIndex)), lstDebugInfo)

        Iterator((pIdx, spatialIndex.asInstanceOf[Any]))
      }
        , preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_ONLY)

    startTime = System.currentTimeMillis

    val numRounds = rddActiveLeft
      .mapPartitions(_.map(computeSquarePoint))
      .distinct
      .mapPartitions(_.map(SpatialIdxOperations.extractLstPartition(bvGlobalIndexRight.value, _, k).length))
      .max

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>LeftDS numRounds time in %,d MS, numRounds: %, d".format(System.currentTimeMillis - startTime, numRounds), lstDebugInfo)

    var rddPoint = rddActiveLeft
      .mapPartitions(iter => {

        var insertPos = 0

        iter.map(point => {

          //                if (point.userData.toString.equalsIgnoreCase("Yellow_1_B_142099"))
          //                  println

          val lstPartitionId = SpatialIdxOperations.extractLstPartition(bvGlobalIndexRight.value, computeSquarePoint(point), k)

          //        if (lstPartitionId.length > 4)
          //          println(SpatialIdxOperations.extractLstPartition(bvGlobalIndexRight.value, computeSquarePoint(point), k))

          // randomize list's tail but maintain order
          while (lstPartitionId.length < numRounds) {

            lstPartitionId.insert(if (insertPos >= lstPartitionId.length) lstPartitionId.length else insertPos, -1)

            insertPos = (insertPos + 1) % numRounds
          }

          val rDataPoint: Any = new RowData(point, SortedLinkedList[Point](k), lstPartitionId.tail)

          (lstPartitionId.head, rDataPoint)
        })
      })

    (0 until numRounds).foreach(roundNum => {
      rddPoint = rddSpIdx
        .union(new ShuffledRDD(rddPoint, rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          // first entry is always the spatial index
          val spatialIndex = iter.next._2 match {
            case spIdx: SpatialIndex => spIdx
          }

          var startTime = -1L
          var counterRow = 0L
          var counterKnn = 0L

          iter.map(row => {

            counterRow += 1

            if (startTime == -1)
              startTime = System.currentTimeMillis()

            row._2 match {
              case rowData: RowData =>

                if (row._1 != -1) {
                  //                                    if (rowData.point.userData.toString.equalsIgnoreCase("bus_1_a_855565"))
                  //                                      println(pIdx)
                  counterKnn += 1
                  spatialIndex.nearestNeighbor(rowData.point, rowData.sortedList)
                }

                if (!iter.hasNext)
                  Helper.loggerSLf4J(debugMode, SparkKnn, ">>kNN done pIdx: %,d round: %,d points: %,d lookup: %,d in %,d MS".format(pIdx, roundNum, counterRow, counterKnn, System.currentTimeMillis() - startTime), lstDebugInfo)

                (rowData.nextPartId, rowData)
            }
          })
        })

      bvGlobalIndexRight.unpersist()
      bvArrPartitionMBRs.unpersist()
    })

    rddPoint.map(_._2 match {
      case rowData: RowData => (rowData.point, rowData.sortedList.iterator.map(nd => (nd.distance, nd.data)))
    })
  }

  private def computeCapacity(rddPoint: RDD[Point], isAllKnn: Boolean): (Long, Int, (Double, Double, Double, Double)) = {

    var startTime = System.currentTimeMillis

    val (maxRowSize, mbrLeft, mbrBottom, mbrRight, mbrTop, mapGrid) =
      rddPoint
        .mapPartitions(_.map(point => (point.userData.toString.length, point.x, point.y, point.x, point.y, mutable.Map(((math.floor(point.x / INITIAL_GRID_WIDTH).toFloat, math.floor(point.y / INITIAL_GRID_WIDTH).toFloat), 1L)))))
        .reduce((param1, param2) => {

          param2._6.foreach(kv => param1._6 += ((kv._1, kv._2 + param1._6.getOrElse(kv._1, 0L))))

          (Helper.max(param1._1, param2._1), Helper.min(param1._2, param2._2), Helper.min(param1._3, param2._3), Helper.max(param1._4, param2._4), Helper.max(param1._5, param2._5), param1._6)
        })

    // compute global index grid box dimension
    var rowCount = 0L
    val getX = (mbrRight - mbrLeft) < (mbrTop - mbrBottom)
    val mapGridCoord = mutable.Map[Float, Long]()

    mapGrid.foreach(kv => {

      rowCount += kv._2

      val key = if (getX) kv._1._1 else kv._1._2

      mapGridCoord += ((key, kv._2 + mapGridCoord.getOrElse(key, 0L)))
    })

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS info time in %,d MS".format(System.currentTimeMillis - startTime), lstDebugInfo)
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>maxRowSize:%,d\trowCount:%,d\tmbrLeft:%.8f\tmbrBottom:%.8f\tmbrRight:%.8f\tmbrTop:%.8f".format(maxRowSize, rowCount, mbrLeft, mbrBottom, mbrRight, mbrTop), lstDebugInfo)

    startTime = System.currentTimeMillis

    //    val driverMemory = Helper.toByte(rddPoint.context.getConf.get("spark.driver.memory"))
    val numExecutors = rddPoint.context.getConf.get("spark.executor.instances").toInt
    val execAvailableMemory = Helper.toByte(rddPoint.context.getConf.get("spark.executor.memory"))
    val execOverheadMemory = Helper.max(384, 0.1 * execAvailableMemory).toLong // 10% reduction in memory to account for yarn overhead
    var coresPerExecutor = rddPoint.context.getConf.get("spark.executor.cores").toDouble
    coresPerExecutor = (numExecutors * coresPerExecutor - 1.0) / numExecutors

    //  Mock objects for size estimate
    val rectMock = Rectangle(new Geom2D(), new Geom2D())
    val pointMock = new Point(0, 0, Array.fill[Char](maxRowSize)(' ').mkString(""))
    val sortListMock = SortedLinkedList[Point](k)
    val lstPartitionIdMock = ListBuffer.fill[Int](rddPoint.getNumPartitions)(0)
    val rowDataMock = new RowData(pointMock, sortListMock, lstPartitionIdMock)
    val spatialIndexMock = SupportedSpatialIndexes(spatialIndexType)

    val pointCost = SizeEstimator.estimate(pointMock)
    val rowDataCost = SizeEstimator.estimate(rowDataMock) + (pointCost * k)
    val spatialIndexCost = SizeEstimator.estimate(spatialIndexMock) + SizeEstimator.estimate(rectMock) /*+
      (spatialIndexMock.estimateNodeCount(rowCount) * SizeEstimator.estimate(spatialIndexMock.mockNode))*/

    var coreObjCapacity = ((((execAvailableMemory - execOverheadMemory) / coresPerExecutor) - spatialIndexCost - rowDataCost) / pointCost).toLong

    var numPartitions = math.ceil(rowCount / coreObjCapacity)

    if (numPartitions < coresPerExecutor) {

      numPartitions = coresPerExecutor.toInt

      coreObjCapacity = ((if (isAllKnn) rowCount * 2 else rowCount) / numPartitions).toLong

      if (isAllKnn)
        coreObjCapacity /= 2
    }

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>coreObjCapacity time in %,d MS".format(System.currentTimeMillis - startTime), lstDebugInfo)
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>numExecutors=%,d coresPerExecutor=%,.2f execAvailableMemory=%,d execOverheadMemory=%,d coreObjCapacity=%,d numPartitions=%,.1f pointCost=%,d rowDataCost=%,d spatialIndexCost=%,d"
      .format(numExecutors, coresPerExecutor, execAvailableMemory, execOverheadMemory, coreObjCapacity, numPartitions, pointCost, rowDataCost, spatialIndexCost), lstDebugInfo)

    var currRange: (Float, Long) = null

    var assignedRowCount = 0L

    val densestRage = mapGridCoord
      .toArray
      .sortBy(_._1)
      .map(row => {

        if (currRange == null)
          currRange = (row._1, 0)

        var rowTmp = row
        val rounds = (rowTmp._2 / coreObjCapacity).toInt + (if (currRange._2 + rowTmp._2 > coreObjCapacity) 1 else 0)

        (0 to rounds).map(_ =>
          currRange._2 + rowTmp._2 match {

            case newCount if newCount < coreObjCapacity =>
              currRange = (currRange._1, newCount)

              assignedRowCount += newCount

              if (assignedRowCount == rowCount)
                (((if (getX) mbrRight else mbrTop) / INITIAL_GRID_WIDTH).toFloat - currRange._1, newCount)
              else
                null

            case newCount if newCount == coreObjCapacity =>
              val ret = (rowTmp._1 - currRange._1, newCount) // sorted in descending order
              currRange = null

              ret

            case _ =>
              val need = coreObjCapacity - currRange._2
              val newEnd = rowTmp._1 + (need / rowTmp._2.toFloat)

              val ret = (newEnd - currRange._1, currRange._2 + need)

              currRange = (newEnd, 0)
              rowTmp = (newEnd, rowTmp._2 - need)
              ret
          }
        )
          .filter(_ != null)
      })
      .flatMap(_.seq)
      //      .map(x => {
      //
      //        println(">>\t" + x)
      //        x
      //      })
      .minBy(wc => wc._1 / wc._2)

    val squareDim = math.ceil(densestRage._1 * INITIAL_GRID_WIDTH / (if (k > 2) math.ceil(Helper.log2(k)) else 2)).toInt

    (coreObjCapacity, squareDim, (math.floor(mbrLeft), math.floor(mbrBottom), math.ceil(mbrRight), math.ceil(mbrTop)))
  }
}