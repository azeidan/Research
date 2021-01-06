package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.sortset.SortedLinkedList
import org.cusp.bdi.sknn.SparkKnn.INITIAL_GRID_WIDTH
import org.cusp.bdi.sknn.ds.util.{GlobalIndexPoint, SpatialIdxOperations, SupportedSpatialIndexes}
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random


case class SparkKnn_23_Uses_RepartitionAndSort(debugMode: Boolean, spatialIndexType: SupportedSpatialIndexes.Value, rddLeft: RDD[Point], rddRight: RDD[Point], k: Int) extends Serializable {

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
          .map(row => new Point(row._1, GlobalIndexPoint(row._2, partCounter)))
      })
        .flatMap(_.seq)
        .view
        .iterator, 1)

    val bvGlobalIndexRight = rddActiveRight.context.broadcast(globalIndexRight)
    val bvArrPartitionMBRs = rddActiveRight.context.broadcast(stackRangeInfo.map(_.mbr).toArray)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>GlobalIndex insert time in %,d MS. Grid size: (%,d X %,d)\tIndex: %s\tIndex Size: %,d".format(System.currentTimeMillis - startTime, squareDimRight, squareDimRight, bvGlobalIndexRight.value, SizeEstimator.estimate(bvGlobalIndexRight.value)), lstDebugInfo)

    val partitioner = new Partitioner() { // group points

      override def numPartitions: Int = bvArrPartitionMBRs.value.length

      override def getPartition(key: Any): Int = key match { // this partitioner is used when partitioning rddPoint
        case keyPartIdx: KeyPartIdx => if (keyPartIdx.pIdx == -1) Random.nextInt(numPartitions) else keyPartIdx.pIdx
        case pIdx: Int => pIdx
      }
    }

    // build a spatial index on each partition
    val rddSpIdx = rddActiveRight
      .mapPartitions(_.map(point => (bvGlobalIndexRight.value.findExact(computeSquarePoint(point)).userData match {
        case globalIndexPoint: GlobalIndexPoint => globalIndexPoint.partitionIdx
      }, point)))
      .partitionBy(partitioner)
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        val startTime = System.currentTimeMillis
        val mbr = bvArrPartitionMBRs.value(pIdx)

        val minX = mbr._1 * squareDimRight + mbrDS_R._1
        val minY = mbr._2 * squareDimRight + mbrDS_R._2
        val maxX = mbr._3 * squareDimRight + mbrDS_R._3 + squareDimRight
        val maxY = mbr._4 * squareDimRight + mbrDS_R._4 + squareDimRight

        val spatialIndex = SupportedSpatialIndexes(spatialIndexType)

        spatialIndex.insert(buildRectBounds((minX, minY), (maxX, maxY)), iter.map(_._2), squareDimRight)

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %,d time in %,d MS. Index: %s. Total Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, /*SizeEstimator.estimate(spatialIndex)*/ -1), lstDebugInfo)

        val key: KeyPartIdx = new KeyPartIdxSpIndex(pIdx)

        Iterator((key, spatialIndex.asInstanceOf[Any]))
      }
        /*, preservesPartitioning = false*/)
    //      .persist(StorageLevel.MEMORY_ONLY)

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
          val key: KeyPartIdx = new KeyPartIdxSpObject(lstPartitionId.head)

          (key, rDataPoint)
        })
      })
    //      .partitionBy(rddSpIdx.partitioner.get)

    rddPoint = (rddSpIdx ++ rddPoint).persist

    (0 until numRounds).foreach(roundNum => {

      //      rddPoint = rddPoint.partitionBy(rddSpIdx.partitioner.get)

      rddPoint = rddPoint
        .partitionBy(partitioner)
        .mapPartitionsWithIndex((pIdx, iter) => {

          //          Helper.loggerSLf4J(debugMode, SparkKnn, ">>Pre index %d roundNum: %d".format(pIdx, roundNum), lstDebugInfo)

          // first entry is always the spatial index
          //          val spatialIndex = iter.next._2 match {
          //            case spIdx: SpatialIndex => spIdx
          ////            case _=>
          ////              println(pIdx)
          ////              SupportedSpatialIndexes(null)
          //          }

          //          Helper.loggerSLf4J(debugMode, SparkKnn, ">>Post index %d roundNum: %d".format(pIdx, roundNum), lstDebugInfo)

          var startTime = -1L
          var counterRow = 0L
          var counterKnn = 0L

          var spatialIndex: SpatialIndex = null

          iter.map(row =>
            row._1 match {
              case _: KeyPartIdxSpIndex =>
                spatialIndex = row._2 match {
                  case sIdx: SpatialIndex => sIdx
                }
                row
              case _ =>

                val rowData = row._2 match {
                  case rd: RowData => rd
                }

                counterRow += 1

                if (startTime == -1)
                  startTime = System.currentTimeMillis()
//                if (spatialIndex == null)
//                  println
                if (row._1 != -1) {
                  //                                    if (rowData.point.userData.toString.equalsIgnoreCase("bus_1_a_855565"))
                  //                                      println(pIdx)
                  counterKnn += 1
                  spatialIndex.nearestNeighbor(rowData.point, rowData.sortedList)
                }

                if (!iter.hasNext)
                  Helper.loggerSLf4J(debugMode, SparkKnn, ">>kNN done pIdx: %,d round: %,d points: %,d lookup: %,d in %,d MS".format(pIdx, roundNum, counterRow, counterKnn, System.currentTimeMillis() - startTime), lstDebugInfo)

                val key: KeyPartIdx = new KeyPartIdxSpObject(rowData.nextPartId)

                (key, rowData.asInstanceOf[Any])
            }
          )
        })

      //      rddPoint = rddSpIdx
      //        .union(new ShuffledRDD(rddPoint, rddSpIdx.partitioner.get))

      bvGlobalIndexRight.unpersist()
      bvArrPartitionMBRs.unpersist()
    })

    //    (0 until numRounds).foreach(roundNum => {
    //
    //      rddPoint = rddSpIdx
    //        .union(new ShuffledRDD(rddPoint, rddSpIdx.partitioner.get))
    //        .mapPartitionsWithIndex((pIdx, iter) => {
    //          Helper.loggerSLf4J(debugMode, SparkKnn, ">>Pre index %d roundNum: %d".format(pIdx, roundNum), lstDebugInfo)
    //          // first entry is always the spatial index
    //          val spatialIndex = iter.next._2 match {
    //            case spIdx: SpatialIndex => spIdx
    //          }
    //          Helper.loggerSLf4J(debugMode, SparkKnn, ">>Post index %d roundNum: %d".format(pIdx, roundNum), lstDebugInfo)
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
    //                  //                                    if (rowData.point.userData.toString.equalsIgnoreCase("bus_1_a_855565"))
    //                  //                                      println(pIdx)
    //                  counterKnn += 1
    //                  spatialIndex.nearestNeighbor(rowData.point, rowData.sortedList)
    //                }
    //
    //                if (!iter.hasNext)
    //                  Helper.loggerSLf4J(debugMode, SparkKnn, ">>kNN done pIdx: %,d round: %,d points: %,d lookup: %,d in %,d MS".format(pIdx, roundNum, counterRow, counterKnn, System.currentTimeMillis() - startTime), lstDebugInfo)
    //
    //                (rowData.nextPartId, rowData.asInstanceOf[Any])
    //            }
    //          })
    //        })
    //
    //      bvGlobalIndexRight.unpersist()
    //      bvArrPartitionMBRs.unpersist()
    //    })

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
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>maxRowSize:%,d\trowCount:%,d\tmbrLeft:%,.8f\tmbrBottom:%,.8f\tmbrRight:%,.8f\tmbrTop:%,.8f".format(maxRowSize, rowCount, mbrLeft, mbrBottom, mbrRight, mbrTop), lstDebugInfo)

    startTime = System.currentTimeMillis

    //    val driverMemory = Helper.toByte(rddPoint.context.getConf.get("spark.driver.memory"))
    val numExecutors = rddPoint.context.getConf.get("spark.executor.instances").toInt
    val execAvailMem = Helper.toByte(rddPoint.context.getConf.get("spark.executor.memory"))
    val execOverheadMem = Helper.max(384, 0.1 * execAvailMem).toLong // 10% reduction in memory to account for yarn overhead
    val coresPerExecutor = rddPoint.context.getConf.get("spark.executor.cores").toInt
    val totalAvailCores = numExecutors * coresPerExecutor - 1
    val avgCoresPerExecutor = totalAvailCores / numExecutors.toFloat

    //  Mock objects for size estimate
    val rectMock = Rectangle(new Geom2D(), new Geom2D())
    val pointMock = new Point(0, 0, ("%" + maxRowSize + "s").format(" "))
    val sortListMock = SortedLinkedList[Point](k)
    val lstPartitionIdMock = ListBuffer.fill[Int](rddPoint.getNumPartitions)(0)
    val rowDataMock = new RowData(pointMock, sortListMock, lstPartitionIdMock)
    val spatialIndexMock = SupportedSpatialIndexes(spatialIndexType)

    val spatialIndexCost = SizeEstimator.estimate(spatialIndexMock) // empty SI size

    spatialIndexMock.insert(rectMock, Iterator(pointMock), 1)

    val objCost = SizeEstimator.estimate(spatialIndexMock) - spatialIndexCost
    val rowDataCost = SizeEstimator.estimate(rowDataMock) + objCost * k
    //    val spatialIndexCost = SizeEstimator.estimate(spatialIndexMock) + SizeEstimator.estimate(rectMock) /*+
    //      (spatialIndexMock.estimateNodeCount(rowCount) * SizeEstimator.estimate(spatialIndexMock.mockNode))*/

    // SI_size = #pts * point_cost + #SI_nodes * (SI_cost + rect_cost)
    // #pts = (SI_size - #SI_nodes * (SI_cost + rect_cost)) / point_cost
    val maxInMemObjCapacity = (((execAvailMem - execOverheadMem - rowDataCost * totalAvailCores) - (spatialIndexMock.estimateNodeCount(rowCount) * (spatialIndexCost + SizeEstimator.estimate(rectMock)))) / objCost.toDouble).toLong

    var coreObjCapacity = maxInMemObjCapacity / totalAvailCores

    var numPartitions = math.ceil(rowCount / coreObjCapacity)

    if (numPartitions < avgCoresPerExecutor) {

      numPartitions = avgCoresPerExecutor.toInt

      coreObjCapacity = ((if (isAllKnn) rowCount * 2 else rowCount) / numPartitions).toLong

      if (isAllKnn)
        coreObjCapacity /= 2
    }

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>coreObjCapacity time in %,d MS".format(System.currentTimeMillis - startTime), lstDebugInfo)
    Helper.loggerSLf4J(debugMode, SparkKnn, ">>numExecutors=%,d coresPerExecutor=%,.2f execAvailMem=%,d execOverheadMem=%,d coreObjCapacity=%,d numPartitions=%,.1f objCost=%,d rowDataCost=%,d spatialIndexCost=%,d"
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