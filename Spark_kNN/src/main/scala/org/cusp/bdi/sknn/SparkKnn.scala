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
import org.cusp.bdi.sknn.SparkKnn.{EXECUTOR_OVERHEAD, EXECUTOR_OVERHEAD_MIN, fComputeGridXY_Coord, fComputeGridXY_Point}
import org.cusp.bdi.sknn.ds.util.SpatialIdxOperations.fCastToPartitionerPointUserData
import org.cusp.bdi.sknn.ds.util.{PartitionerPointUserData, SpatialIdxOperations, SupportedSpatialIndexes}
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object SparkKnn extends Serializable {

  // Percentage of Executor memory to compute the memory overhead
  val EXECUTOR_OVERHEAD: Double = 0.10 // Per the Spark configuration

  // The executor's minimum overhead memory (in bytes)
  val EXECUTOR_OVERHEAD_MIN: Double = 384e6 // Per the Spark configuration

  def fComputeGridXY_Point: (Point, Int) => (Int, Int) = (point: Point, gridDim: Int) => fComputeGridXY_Coord(point.x, point.y, gridDim)

  def fComputeGridXY_Coord: (Double, Double, Int) => (Int, Int) = (x: Double, y: Double, gridDim: Int) => (Helper.round(x / gridDim), Helper.round(y / gridDim))

  def getSparkKNNClasses: Array[Class[_]] =
    Array(
      Helper.getClass,
      SupportedKnnOperations.getClass,
      classOf[SortedLinkedList[_]],
      classOf[Rectangle],
      classOf[Circle],
      classOf[Point],
      classOf[MBR],
      classOf[Geom2D],
      classOf[PartitionerPointUserData],
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
      classOf[mutable.Queue[MBR]])
}

case class SparkKnn(debugMode: Boolean, spatialIndexType: SupportedSpatialIndexes.Value, rddLeft: RDD[Point], rddRight: RDD[Point], k: Int, initialGridDim: Int, partitionMaxByteSize: Long) extends Serializable {

  val lstDebugInfo: ListBuffer[String] = ListBuffer()

  def knnJoin(): RDD[(Point, Iterable[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoin", lstDebugInfo)

    val (partObjCapacityRight, mbrRight, gridDimRight) = computeCapacity(rddRight, rddLeft, isAllKnn = false)

    knnJoinExecute(rddLeft, rddRight, mbrRight, partObjCapacityRight, gridDimRight)
  }

  def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] = {

    val (partObjCapacityRight, mbrRight, gridDimRight) = computeCapacity(rddRight, rddLeft, isAllKnn = true)
    val (partObjCapacityLeft, mbrLeft, gridDimLeft) = computeCapacity(rddLeft, rddRight, isAllKnn = true)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>All kNN partObjCapacityLeft=%s partObjCapacityRight=%s".format(partObjCapacityLeft, partObjCapacityRight), lstDebugInfo)

    knnJoinExecute(rddRight, rddLeft, mbrLeft, partObjCapacityLeft, gridDimLeft)
      .union(knnJoinExecute(rddLeft, rddRight, mbrRight, partObjCapacityRight, gridDimRight))
  }

  private def knnJoinExecute(rddActiveLeft: RDD[Point], rddActiveRight: RDD[Point], mbrDS_ActiveRight: MBR, coreObjCapacity: (Long, Long), gridDimRightActive: Int): RDD[(Point, Iterable[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>knnJoinExecute", lstDebugInfo)

    var startTime = System.currentTimeMillis
    var bvArrPartitionMBRs_ActiveRight: Broadcast[Array[MBR]] = null
    var bvPartitioner_ActiveRight: Broadcast[SpatialIndex] = null

    {
      val queueRangeInfo = mutable.Queue[MBR]()
      var currObjCountMBR = 0L
      var partCounter = -1

      // build range info
      val iterPartitionerObjects = rddActiveRight
        .mapPartitions(_.map(point => (fComputeGridXY_Point(point, gridDimRightActive), 1L))) // grid assignment
        .reduceByKey(_ + _) // summarize
        .sortByKey() // sorts by (x, y)
        .collect()
        .toStream
        .map(row => { // group cells on partitions

          val newObjCountMBR = currObjCountMBR + row._2

          if (currObjCountMBR == 0 || currObjCountMBR >= coreObjCapacity._1 || newObjCountMBR > coreObjCapacity._2) {

            partCounter += 1
            currObjCountMBR = row._2
            queueRangeInfo += new MBR(row._1)
          }
          else {

            currObjCountMBR = newObjCountMBR
            queueRangeInfo.last.update(row._1)
          }

          new Point(row._1._1, row._1._2, new PartitionerPointUserData(row._2, partCounter))
        })
        .iterator

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>rangeInfo time in %,d MS.".format(System.currentTimeMillis - startTime), lstDebugInfo)

      startTime = System.currentTimeMillis

      // create partitioner
      val partitioner_ActiveRight = SupportedSpatialIndexes(spatialIndexType)
      partitioner_ActiveRight.insert(buildRectBounds(fComputeGridXY_Coord(mbrDS_ActiveRight.left, mbrDS_ActiveRight.bottom, gridDimRightActive), fComputeGridXY_Coord(mbrDS_ActiveRight.right, mbrDS_ActiveRight.top, gridDimRightActive)), iterPartitionerObjects, 1)

      bvPartitioner_ActiveRight = rddActiveRight.context.broadcast(partitioner_ActiveRight)

      bvArrPartitionMBRs_ActiveRight = rddActiveRight.context.broadcast(queueRangeInfo.toArray)

      Helper.loggerSLf4J(debugMode, SparkKnn, ">>Partitioner insert time in %,d MS. Grid size: (%,d X %,d)\tIndex: %s\tIndex Size: %,d".format(System.currentTimeMillis - startTime, gridDimRightActive, gridDimRightActive, partitioner_ActiveRight, -1 /*SizeEstimator.estimate(partitioner_ActiveRight)*/), lstDebugInfo)

      queueRangeInfo.foreach(row =>
        Helper.loggerSLf4J(debugMode, SparkKnn, ">>\t%s".format(row.toString), lstDebugInfo))
    }

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Actual number of partitions: %,d".format(bvArrPartitionMBRs_ActiveRight.value.length), lstDebugInfo)

    startTime = System.currentTimeMillis

    val numRounds = rddActiveLeft
      .mapPartitions(_.map(point => (fComputeGridXY_Point(point, gridDimRightActive), null)))
      .reduceByKey((_, _) => null) // distinct
      .mapPartitions(_.map(row => SpatialIdxOperations.extractLstPartition(bvPartitioner_ActiveRight.value, row._1, k).length))
      .max

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>LeftDS numRounds done. numRounds: %,d time in %,d MS".format(numRounds, System.currentTimeMillis - startTime), lstDebugInfo)

    // build a spatial index on each partition
    val rddSpIdx: RDD[(Int, AnyRef)] = rddActiveRight
      .mapPartitions(_.map(point => {
        // the (actualNumPartitions - 1 - arr index) due to the stack. Points are added in reverse order!
        val gridXY = fComputeGridXY_Point(point, gridDimRightActive)

        (fCastToPartitionerPointUserData(bvPartitioner_ActiveRight.value.findExact(gridXY._1, gridXY._2)).partitionIdx, point)
      }))
      .partitionBy(new Partitioner() {

        override def numPartitions: Int = bvArrPartitionMBRs_ActiveRight.value.length

        override def getPartition(key: Any): Int =
          key match {
            case pIdx: Int => if (pIdx < 0) -pIdx - 1 else pIdx // -1 to undo the +1 during random assignment of leftRDD (there is no -0)
          }
      })
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        val startTime = System.currentTimeMillis
        //        val mbrPartition = bvArrPartitionMBRs_ActiveRight.value(bvArrPartitionMBRs_ActiveRight.value.length - 1 - pIdx)
        val mbrPartition = bvArrPartitionMBRs_ActiveRight.value(pIdx)
        val rectSI = buildRectBounds(mbrPartition.left * gridDimRightActive - gridDimRightActive, mbrPartition.bottom * gridDimRightActive - gridDimRightActive, mbrPartition.right * gridDimRightActive + gridDimRightActive, mbrPartition.top * gridDimRightActive + gridDimRightActive)

        val spatialIndex = SupportedSpatialIndexes(spatialIndexType)

        spatialIndex.insert(rectSI, iter.map(_._2), gridDimRightActive)

        Helper.loggerSLf4J(debugMode, SparkKnn, ">>SpatialIndex on partition %,d time in %,d MS. Index: %s\tTotal Size: %,d".format(pIdx, System.currentTimeMillis - startTime, spatialIndex, -1 /*SizeEstimator.estimate(spatialIndex)*/), lstDebugInfo)

        Iterator((pIdx, spatialIndex.asInstanceOf[AnyRef]))
      }, preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_AND_DISK)

    var rddPoint: RDD[(Int, AnyRef)] = rddActiveLeft
      .mapPartitions(iter => {

        val rand = Random

        iter.map(point => {

          val arrPartitionId = SpatialIdxOperations.extractLstPartition(bvPartitioner_ActiveRight.value, fComputeGridXY_Point(point, gridDimRightActive), k)

          if (arrPartitionId.length < numRounds) {

            val setSelectedPartitions = arrPartitionId.to[mutable.Set]

            arrPartitionId.sizeHint(numRounds)

            while (arrPartitionId.length < numRounds) {

              var randPartIdx = 0

              do
                randPartIdx = rand.nextInt(bvArrPartitionMBRs_ActiveRight.value.length)
              while (!setSelectedPartitions.add(randPartIdx))

              arrPartitionId.insert(rand.nextInt(arrPartitionId.length + 1), -(randPartIdx + 1)) // +1 since there is no signed 0, - to indicate a skip partition
            }
          }

          //        println(">>>\t" + arrPartitionId.mkString("\t"))

          (arrPartitionId.head, new RowData(point, new SortedLinkedList[Point](k), arrPartitionId.tail))
        })
      })

    for (currRoundNum <- 1 to numRounds) {
      rddPoint = (rddSpIdx ++ rddPoint.partitionBy(rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          var counterPoints = 0L
          var counterKNN = 0L

          // first entry is always the spatial index
          val spatialIndex: SpatialIndex = iter.next()._2 match {
            case spIdx: SpatialIndex =>
              spIdx
          }

          iter.map(row =>
            row._2 match {
              case rowData: RowData =>

                counterPoints += 1

                if (row._1 >= 0) {

                  counterKNN += 1
                  spatialIndex.nearestNeighbor(rowData.point, rowData.sortedList)
                }

                if (!iter.hasNext)
                  Helper.loggerSLf4J(debugMode, SparkKnn, ">>kNN done index: %,d roundNum: %,d numPoints: %,d counterKNN: %,d".format(pIdx, currRoundNum, counterPoints, counterKNN), lstDebugInfo)

                (rowData.nextPartId(), rowData)
            })
        })

      if (currRoundNum == 1) {

        bvArrPartitionMBRs_ActiveRight.unpersist(false)
        bvPartitioner_ActiveRight.unpersist(false)
      }
    }

    rddPoint
      .mapPartitions(_.map(_._2 match {
        case rowData: RowData => (rowData.point, rowData.sortedList.map(node => (Math.sqrt(node.distance), node.data)))
      }))
  }

  /**
     * Stage 1 for constructing a dataset partitioner - Analyzing the input datasets
     * @param rddRight, the right RDD
     * @param rddLeft, the left RDD
     * @param isAllKnn, true if this is an all kNN operation. The resources will be divided by 2
     *
     * @return a tuple consisting of (1) the partition point capacity range (min-max), (2) the MBR of the right dataset, (3) the adjusted grid width
    */
  private def computeCapacity(rddRight: RDD[Point], rddLeft: RDD[Point], isAllKnn: Boolean): ((Long, Long), MBR, Int) = {

    val countExec = rddRight.context.getConf.get("spark.executor.instances").toInt
    val memExec = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory"))
    val memOverheadExec = Helper.max(EXECUTOR_OVERHEAD_MIN, EXECUTOR_OVERHEAD * memExec).toLong
    val countExecCores = rddRight.context.getConf.get("spark.executor.cores").toInt
    val countAllCores = countExec * countExecCores
    val coreAvailMem = (memExec - memOverheadExec) / countExecCores
    //    val memMaxPart = Helper.min(coreAvailMem, partitionMaxByteSize).toDouble
    val memMaxPart = ((memExec - memOverheadExec) / countExecCores).toDouble

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>countExec: %,d memExec: %,d memOverheadExec: %,d countExecCores: %,d countAllCores: %,d coreAvailMem: %,d memMaxPart: %,.2f"
      .format(countExec, memExec, memOverheadExec, countExecCores, countAllCores, coreAvailMem, memMaxPart), lstDebugInfo)

    var startTime = System.currentTimeMillis

    // Analyze Right RDD
    val (memMaxPointRight, mbrRight, countPointSquareRight, countSquareCells, countPointRight) = rddRight
      .mapPartitions(_.map(point => (fComputeGridXY_Point(point, initialGridDim), (SizeEstimator.estimate(point), new MBR(point.x.toInt, point.y.toInt), 1L))))
      .reduceByKey((row1, row2) => (Helper.max(row1._1, row2._1), row1._2.merge(row2._2), row1._3 + row2._3)) // gets rid of duplicates but keeps track of the actual MBR and count
      .mapPartitions(_.map(row => (row._2._1, row._2._2, row._2._3, 1L, row._2._3))) // necessary to introduce total row count
      .treeReduce((row1, row2) => (Helper.max(row1._1, row2._1), row1._2.merge(row2._2), Helper.max(row1._3, row2._3), row1._4 + row2._4, row1._5 + row2._5)) // aggregate results

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS info done memMaxPointRight: %,d mbrRight: %s countPointSquareRight: %,d countSquareCells: %,d countPointRight: %,d Time: %,d MS"
      .format(memMaxPointRight, mbrRight, countPointSquareRight, countSquareCells, countPointRight, System.currentTimeMillis - startTime), lstDebugInfo)

    val memRect = SizeEstimator.estimate(new Rectangle(new Geom2D(), new Geom2D()))

    /*
     * Right dataset (Spatial Indexes) size estimate. Every row in the right RDD contains:
     * 1. partition ID (int) <- insignificant here, so it's not accounted for.
     * 2. spatial index: # of nodes with objects
    */
    val spIdxMockRight = SupportedSpatialIndexes(spatialIndexType)
    val memSpIdxMockRight = SizeEstimator.estimate(spIdxMockRight)
    spIdxMockRight.insert(new Rectangle(new Geom2D(Double.MaxValue / 2)), (0 until spIdxMockRight.nodeCapacity).map(new Point(_)).iterator, 1)
    val memSpIdxInsertOverheadRight = (SizeEstimator.estimate(spIdxMockRight) - memSpIdxMockRight - memRect) / spIdxMockRight.nodeCapacity.toDouble

    val countNodesSpIdxRight = spIdxMockRight.estimateNodeCount(countPointRight)

    val memRightRDD = (countPointRight * memMaxPointRight) + (countNodesSpIdxRight * (memSpIdxMockRight + memRect + memSpIdxInsertOverheadRight)) * (if (isAllKnn) 2 else 1)

    val countPartitionRight = Math.ceil(memRightRDD / memMaxPart).toInt

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Right DS memSpIdxMockRight: %,d memSpIdxInsertOverheadRight: %,.2f memRect: %,d countNodesSpIdxRight: %,d memRightRDD: %,.2f countPartitionRight: %,d"
      .format(memSpIdxMockRight, memSpIdxInsertOverheadRight, memRect, countNodesSpIdxRight, memRightRDD, countPartitionRight), lstDebugInfo)

    /*
     * every row in the left RDD contains:
     *   1. partition ID (int)
     *   2. point info (RowData -> (point, SortedLinkedList, list of partitions to visit))
     *
     *   point: coordinates + userData <- from raw file
     *   SortedLinkedList: point matches from the right RDD of size up to k
     */

    startTime = System.currentTimeMillis

    val (memMaxPointLeft, countPointLeft) = rddLeft
      .mapPartitions(_.map(point => (SizeEstimator.estimate(point), 1L)))
      .treeReduce((row1, row2) => (Helper.max(row1._1, row2._1), row1._2 + row2._2))

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Left DS info done memMaxPointLeft: %,d countPointLeft: %,d Time: %,d MS"
      .format(memMaxPointLeft, countPointLeft, System.currentTimeMillis - startTime), lstDebugInfo)

    val arrPartIdMockLeft = ArrayBuffer.fill[Int](Helper.max(countPartitionRight, countAllCores))(0)
    val memArrPartIdLeft = SizeEstimator.estimate(arrPartIdMockLeft)

    val sortLstMockLeft = new SortedLinkedList[Point](k) // at the end of the process, the SortedLinkedList contains k points from the right DS
    var memSortLstLeft = SizeEstimator.estimate(sortLstMockLeft)

    sortLstMockLeft.add(0, Point())
    val memSortLstInsertOverhead = SizeEstimator.estimate(sortLstMockLeft) - SizeEstimator.estimate(Point()) - memSortLstLeft + memMaxPointRight
    memSortLstLeft += k * memSortLstInsertOverhead

    val tupleMockLeft = (0, new RowData())
    val memTupleObjLeft = SizeEstimator.estimate(tupleMockLeft) + memMaxPointLeft + memArrPartIdLeft + memSortLstLeft

    val memLeftRDD = countPointLeft * memTupleObjLeft * (if (isAllKnn) 2 else 1)
    val countPartitionsLeft = Math.ceil(memLeftRDD / memMaxPart)

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>Left DS memMaxPointLeft: %,d memSortLstLeft: %,d memSortLstInsertOverhead: %,d memTupleObjLeft: %,d memLeftRDD: %,d countPartitionsLeft: %,.2f"
      .format(memMaxPointLeft, memSortLstLeft, memSortLstInsertOverhead, memTupleObjLeft, memLeftRDD, countPartitionsLeft), lstDebugInfo)

    val countPartitionMinRight = Helper.max(countPartitionRight, countPartitionsLeft).toInt
    val countPartitionMaxRight = if (countPartitionMinRight < countAllCores) countAllCores else countPartitionMinRight //(Math.ceil(countPartitionMinRight / countAllCores.toDouble) * countAllCores).toLong // a multiple of the total number of cores

    val countPartPointMinMaxRight = (countPointRight / countPartitionMaxRight, countPointRight / countPartitionMinRight)

    // Partitioner MEM estimate
    //    val spIdxMockPartitioner = SupportedSpatialIndexes(spatialIndexType)
    //    val memSPIdxPartitioner = SizeEstimator.estimate(spIdxMockPartitioner)
    //    spIdxMockPartitioner.insert(new Rectangle(new Geom2D(Double.MaxValue / 2)), (0 until spIdxMockPartitioner.nodeCapacity).map(new Point(_)).iterator, 1)
    //    val memSpIdxInsertOverheadPartitioner = (SizeEstimator.estimate(spIdxMockPartitioner) - memSPIdxPartitioner - memRect) / spIdxMockPartitioner.nodeCapacity.toDouble
    //
    //    val countNodesSpIdxPartitioner = spIdxMockPartitioner.estimateNodeCount(countSquareCells)
    //    val memPointPartitioner = SizeEstimator.estimate(new Point(1, 2, new PartitionerPointUserData(3, 4)))
    //    val memPartitioner = (countNodesSpIdxPartitioner * memPointPartitioner) + (countNodesSpIdxPartitioner * (memSPIdxPartitioner + memRect + memSpIdxInsertOverheadPartitioner)) * (if (isAllKnn) 2 else 1)
    //
    //    val memExecFree =

    val rate = countPointSquareRight / countPartPointMinMaxRight._1.toDouble
    val gridDim = if (rate > 1) // maxGridCellCount has more than coreObjCapacity._1
      (initialGridDim / Math.ceil(Math.sqrt(rate))).toInt
    else
      initialGridDim

    Helper.loggerSLf4J(debugMode, SparkKnn, ">>memLeftRDD: %,d countPartitionMinRight: %,d countPartitionMaxRight: %,d countPartPointMinMaxRight: %s rate: %,.4f gridDim: %,d"
      .format(memLeftRDD, countPartitionMinRight, countPartitionMaxRight, countPartPointMinMaxRight, rate, gridDim), lstDebugInfo)

    (countPartPointMinMaxRight, mbrRight, gridDim)
  }
}