package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Circle, Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.{KdTree, KdtBranchRootNode, KdtLeafNode, KdtNode}
import org.cusp.bdi.ds.sortset.{Node, SortedLinkedList}
import org.cusp.bdi.sknn.SparkKnn.INITIAL_GRID_WIDTH
import org.cusp.bdi.sknn.ds.util.{GlobalIndexPoint, SpatialIdxOperations, SupportedSpatialIndexes}
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
      classOf[GlobalIndexPoint],
      classOf[KdTree],
      KdTree.getClass,
      //      classOf[QuadTree],
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
      classOf[Node[_]],
      classOf[ListBuffer[_]])
}

case class SparkKnn(debugMode: Boolean, spatialIndexType: SupportedSpatialIndexes.Value, rddLeft: RDD[Point], rddRight: RDD[Point], k: Int) extends Serializable {

  val lstDebugInfo: ListBuffer[String] = ListBuffer()

  def knnJoin(): RDD[(Point, Iterator[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoin", lstDebugInfo)

    //    rddRight.cache

    val (partObjCapacityRight, squareDimRight, mbrDS) = computeCapacity(rddRight, isAllKnn = false)

    knnJoinExecute(rddLeft, rddRight, mbrDS, SupportedSpatialIndexes(spatialIndexType), squareDimRight, partObjCapacityRight)
  }

  def allKnnJoin(): RDD[(Point, Iterator[(Double, Point)])] = {

    val (partObjCapacityRight, squareDimRight, mbrDS_R) = computeCapacity(rddRight, isAllKnn = true)
    val (partObjCapacityLeft, squareDimLeft, mbrDS_L) = computeCapacity(rddLeft, isAllKnn = true)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>All kNN partObjCapacityLeft=%,d partObjCapacityRight=%,d".format(partObjCapacityLeft, partObjCapacityRight), lstDebugInfo)

    knnJoinExecute(rddRight, rddLeft, mbrDS_L, SupportedSpatialIndexes(spatialIndexType), squareDimLeft, partObjCapacityLeft)
      .union(knnJoinExecute(rddLeft, rddRight, mbrDS_R, SupportedSpatialIndexes(spatialIndexType), squareDimRight, partObjCapacityRight))
  }

  private def knnJoinExecute(rddActiveLeft: RDD[Point],
                             rddActiveRight: RDD[Point], mbrDS_ActiveRight: (Double, Double, Double, Double), gIdxActiveRight: SpatialIndex, gridSquareDimActiveRight: Int,
                             partObjCapacity: Long): RDD[(Point, Iterator[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoinExecute", lstDebugInfo)

    def computeSquareXY(x: Double, y: Double): (Double, Double) = (((x - mbrDS_ActiveRight._1) / gridSquareDimActiveRight).floor, ((y - mbrDS_ActiveRight._2) / gridSquareDimActiveRight).floor)

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

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>rangeInfo time in %,d MS.".format(System.currentTimeMillis - startTime), lstDebugInfo)

    startTime = System.currentTimeMillis

    // create global index
    var partCounter = -1
    gIdxActiveRight.insert(buildRectBounds(computeSquareXY(mbrDS_ActiveRight._1, mbrDS_ActiveRight._2), computeSquareXY(mbrDS_ActiveRight._3, mbrDS_ActiveRight._4)),
      stackRangeInfo.map(rangeInfo => {

        partCounter += 1

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>%s\t%,d".format(rangeInfo.toString, partCounter), lstDebugInfo)

        rangeInfo
          .lstMBRCoord
          .map(row => new Point(row._1, GlobalIndexPoint(row._2, partCounter)))
      })
        .flatMap(_.seq)
        .view
        .iterator, 1)

    val bvGlobalIndexRight = rddActiveRight.context.broadcast(gIdxActiveRight)
    val bvArrPartitionMBRs = rddActiveRight.context.broadcast(stackRangeInfo.map(_.mbr).toArray)

    val actualNumPartitions = bvArrPartitionMBRs.value.length

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Actual number of partitions: %,d".format(actualNumPartitions), lstDebugInfo)
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>GlobalIndex insert time in %,d MS. Grid size: (%,d X %,d)\tIndex: %s\tIndex Size: %,d".format(System.currentTimeMillis - startTime, gridSquareDimActiveRight, gridSquareDimActiveRight, bvGlobalIndexRight.value, SizeEstimator.estimate(bvGlobalIndexRight.value)), lstDebugInfo)

    val partitioner = new Partitioner() { // group points

      override def numPartitions: Int = actualNumPartitions

      override def getPartition(key: Any): Int = key match { // this partitioner is used when partitioning rddPoint
        case pIdx: Int => if (pIdx == -1) Random.nextInt(numPartitions) else pIdx
      }
    }

    // build a spatial index on each partition
    val rddSpIdx = rddActiveRight
      .mapPartitionsWithIndex((pIdx, iter) => {

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>findExact pIdx: %d".format(pIdx), lstDebugInfo)

        iter.map(point => (bvGlobalIndexRight.value.findExact(computeSquarePoint(point)).userData match {
          case globalIndexPoint: GlobalIndexPoint => globalIndexPoint.partitionIdx
        }, point))
      })
      .partitionBy(new Partitioner() { // group points

        override def numPartitions: Int = actualNumPartitions

        override def getPartition(key: Any): Int = key match { // this partitioner is used when partitioning rddPoint
          case pIdx: Int => if (pIdx == -1) Random.nextInt(numPartitions) else pIdx
        }
      })
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        val startTime = System.currentTimeMillis
        val mbr = bvArrPartitionMBRs.value(pIdx)

        val minX = mbr._1 * gridSquareDimActiveRight + mbrDS_ActiveRight._1
        val minY = mbr._2 * gridSquareDimActiveRight + mbrDS_ActiveRight._2
        val maxX = mbr._3 * gridSquareDimActiveRight + mbrDS_ActiveRight._3 + gridSquareDimActiveRight
        val maxY = mbr._4 * gridSquareDimActiveRight + mbrDS_ActiveRight._4 + gridSquareDimActiveRight

        val spatialIndex = SupportedSpatialIndexes(spatialIndexType)

        spatialIndex.insert(buildRectBounds((minX, minY), (maxX, maxY)), iter.map(_._2), gridSquareDimActiveRight)

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %,d time in %,d MS. Index: %s. Total Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, SizeEstimator.estimate(spatialIndex)), lstDebugInfo)

        Iterator((pIdx, spatialIndex.asInstanceOf[Any]))
      }, true)
      .cache()
      .partitionBy(partitioner)

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

          val rDataPoint: Any = new RowData(point, new SortedLinkedList[Point](k), lstPartitionId.tail)

          (lstPartitionId.head, rDataPoint)
        })
      })

    (0 until numRounds).foreach(roundNum => {

      rddPoint = (rddSpIdx ++ new ShuffledRDD(rddPoint, rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          Helper.loggerSLf4J(debugMode, SparkKnn, ">>Pre index %d roundNum: %d".format(pIdx, roundNum), lstDebugInfo)

          // first entry is always the spatial index
          val spatialIndex = iter.next._2 match {
            case spIdx: SpatialIndex => spIdx
          }
          Helper.loggerSLf4J(debugMode, SparkKnn, ">>Post index %d roundNum: %d".format(pIdx, roundNum), lstDebugInfo)
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

                (rowData.nextPartId, rowData.asInstanceOf[Any])
            }
          })
        })

      //      rddActiveRight.unpersist(false)
      bvGlobalIndexRight.unpersist(false)
      bvArrPartitionMBRs.unpersist(false)
    })

    //    rddPoint.saveAsTextFile("/media/cusp/Data/GeoMatch_Files/OutputFiles/TBD/" + Random.nextInt(9999))

    rddPoint
      .mapPartitions(_.map(_._2 match {
        case rowData: RowData => (rowData.point, rowData.sortedList.iterator.map(nd => (nd.distance, nd.data)))
        case _ => null
      }))
      .filter(_ != null)
  }

  private def computeCapacity(rddRight: RDD[Point], isAllKnn: Boolean): (Long, Int, (Double, Double, Double, Double)) = {

    var startTime = System.currentTimeMillis

    val (maxRowSize, mbrLeft, mbrBottom, mbrRight, mbrTop, mapGrid) =
      rddRight
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
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>maxRowSize:%,d\trowCount:%,d\tmbrLeft:%,.8f\tmbrBottom:%,.8f\tmbrRight:%,.8f\tmbrTop:%,.8f".format(maxRowSize, rowCount, mbrLeft, mbrBottom, mbrRight, mbrTop), lstDebugInfo)

    startTime = System.currentTimeMillis

    //    val driverMemory = Helper.toByte(rddPoint.context.getConf.get("spark.driver.memory"))
    val numExecutors = rddRight.context.getConf.get("spark.executor.instances").toInt
    val execAvailMem = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory"))
    val execOverheadMem = Helper.max(384, 0.1 * execAvailMem).toLong // 10% reduction in memory to account for yarn overhead
    val numCoresPerExecutor = rddRight.context.getConf.get("spark.executor.cores").toInt
    val totalAvailCores = numExecutors * numCoresPerExecutor - 1
    val avgCoresPerExecutor = totalAvailCores / numExecutors.toDouble

    //  Mock objects for size estimate
    val rectMock = Rectangle(new Geom2D(), new Geom2D())
    val pointMock = new Point(0, 0, ("%" + maxRowSize + "s").format(" "))
    val sortListMock = new SortedLinkedList[Point](k)
    val lstPartitionIdMock = ListBuffer.fill[Int](rddRight.getNumPartitions)(0)
    val rowDataMock = new RowData(pointMock, sortListMock, lstPartitionIdMock)
    val spatialIndexMock = SupportedSpatialIndexes(spatialIndexType)

    val spatialIndexCost = SizeEstimator.estimate(spatialIndexMock) // empty SI size

    spatialIndexMock.insert(rectMock, Iterator(pointMock), 1)

    val objCost = SizeEstimator.estimate(spatialIndexMock) - spatialIndexCost
    val rowDataCost = SizeEstimator.estimate(rowDataMock) + objCost * k
    val rectCost = SizeEstimator.estimate(rectMock)
    //    val spatialIndexCost = SizeEstimator.estimate(spatialIndexMock) + SizeEstimator.estimate(rectMock) /*+
    //      (spatialIndexMock.estimateNodeCount(rowCount) * SizeEstimator.estimate(spatialIndexMock.mockNode))*/

    // System wide "estimate"
    // SI_size = #pts * point_cost + #SI_nodes * (SI_cost + rect_cost)
    // #pts = (SI_size - #SI_nodes * (SI_cost + rect_cost)) / point_cost

    // <memory avail on each executor> = <job's exec mem> - <overhead> - <cost of one row on each executor)
    val systemAvailMem = numExecutors * (execAvailMem - execOverheadMem - (rowDataCost * numCoresPerExecutor))
    val systemObjCache = (systemAvailMem / objCost.toDouble).toLong
    val totalSpatialIdxCost = spatialIndexMock.estimateNodeCount(systemObjCache) * (spatialIndexCost + rectCost)
    var coreObjCapacity = (systemAvailMem - totalSpatialIdxCost) / objCost / totalAvailCores

    if (isAllKnn)
      coreObjCapacity /= 2

    var numPartitions = math.ceil(rowCount / coreObjCapacity.toDouble)

    if (numPartitions < numCoresPerExecutor) {

      numPartitions = numCoresPerExecutor

      coreObjCapacity = ((if (isAllKnn) rowCount * 2 else rowCount) / numPartitions).toLong

      if (isAllKnn)
        coreObjCapacity /= 2
    }

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>coreObjCapacity time in %,d MS".format(System.currentTimeMillis - startTime), lstDebugInfo)
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>numExecutors=%,d numCoresPerExecutor=%,.2f execAvailMem=%,d execOverheadMem=%,d coreObjCapacity=%,d numPartitions=%,.1f objCost=%,d rowDataCost=%,d spatialIndexCost=%,d"
      .format(numExecutors, avgCoresPerExecutor, execAvailMem, execOverheadMem, coreObjCapacity, numPartitions, objCost, rowDataCost, spatialIndexCost), lstDebugInfo)

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