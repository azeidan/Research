package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Circle, Geom2D, Point, Rectangle}
import org.cusp.bdi.sknn.ds.util.{PointData, SpatialIdxRangeLookup}
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.{Arguments, Helper}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object TypeSpatialIndex extends Enumeration with Serializable {

  val quadTree: TypeSpatialIndex.Value = Value(0.toByte)
  val kdTree: TypeSpatialIndex.Value = Value(1.toByte)
}

case class RangeInfo(seedObj: ((Double, Double), Int)) extends Serializable {

  val lstMBRCoords: ListBuffer[((Double, Double), Int)] = ListBuffer[((Double, Double), Int)]()
  var totalWeight = 0L
  val leftObj: ((Double, Double), Int) = seedObj
  var bottomObj: ((Double, Double), Int) = seedObj
  var rightObj: ((Double, Double), Int) = seedObj
  var topObj: ((Double, Double), Int) = seedObj

  def mbr: (Double, Double, Double, Double) = (leftObj._1._1, bottomObj._1._2, rightObj._1._1, topObj._1._2)

  override def toString: String =
    "%f\t%f\t%f\t%f\t%d".format(leftObj._1._1, bottomObj._1._2, rightObj._1._1, topObj._1._2, totalWeight)
}

case class RowData(point: Point, sortedList: SortedList[Point]) extends Serializable {

  var lstPartitionId: List[Int] = _

  def this(point: Point, sortedList: SortedList[Point], lstPartitionId: List[Int]) {

    this(point, sortedList)

    this.lstPartitionId = lstPartitionId
  }
}

case class GlobalIndexPointData(numPoints: Int, partitionIdx: Int) extends PointData {
  override def equals(other: Any): Boolean = false
}

object SparkKNN extends Serializable {

  def getSparkKNNClasses: Array[Class[_]] =
    Array(
      Helper.getClass,
      classOf[SortedList[_]],
      classOf[Rectangle],
      classOf[Circle],
      classOf[Point],
      classOf[Geom2D],
      classOf[QuadTree],
      classOf[KdTree],
      classOf[KdtNode],
      classOf[KdtBranchRootNode],
      SparkKNN.getClass,
      classOf[SparkKNN],
      classOf[RowData],
      classOf[RangeInfo],
      TypeSpatialIndex.getClass,
      classOf[GlobalIndexPointData],
      classOf[SpatialIndex],
      classOf[GridOperation],
      Arguments.getClass,
      classOf[Node[_]])
}

case class SparkKNN(debugMode: Boolean, k: Int, typeSpatialIndex: TypeSpatialIndex.Value) extends Serializable {

  def knnJoin(rddLeft: RDD[Point], rddRight: RDD[Point]): RDD[(Point, Iterable[(Double, Point)])] =
    knnJoinExecute(rddLeft, rddRight, isAllJoin = false)

  def allKnnJoin(rddLeft: RDD[Point], rddRight: RDD[Point]): RDD[(Point, Iterable[(Double, Point)])] =
    knnJoinExecute(rddLeft, rddRight, isAllJoin = true)
      .union(knnJoinExecute(rddRight, rddLeft, isAllJoin = false))

  var (leftDS_MaxRowSize, leftDS_TotalRowCount, leftDS_MBR_Left, leftDS_MBR_Bottom, leftDS_MBR_Right, leftDS_MBR_Top) = (-1, -1L, -1.0, -1.0, -1.0, -1.0)

  private def knnJoinExecute(rddLeft: RDD[Point], rddRight: RDD[Point], isAllJoin: Boolean): RDD[(Point, Iterable[(Double, Point)])] = {

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>Start")
    Helper.loggerSLf4J(debugMode, SparkKNN, ">>rddLeft.getNumPartitions: %d".format(rddLeft.getNumPartitions))
    Helper.loggerSLf4J(debugMode, SparkKNN, ">>rddRight.getNumPartitions: %d".format(rddRight.getNumPartitions))

    var startTime = System.currentTimeMillis

    val (rightDS_MaxRowSize, rightDS_TotalRowCount, rightDS_MBR_Left, rightDS_MBR_Bottom, rightDS_MBR_Right, rightDS_MBR_Top) =
      if (leftDS_MaxRowSize == -1)
        rddRight.mapPartitions(iter =>
          Iterator(iter.map(point => (point.userData.toString.length, 1L, point.x, point.y, point.x, point.y))
            .reduce((param1, param2) =>
              (math.max(param1._1, param2._1), param1._2 + param2._2, math.min(param1._3, param2._3), math.min(param1._4, param2._4), math.max(param1._5, param2._5), math.max(param1._6, param2._6))))
        )
          .reduce((param1, param2) =>
            (math.max(param1._1, param2._1), param1._2 + param2._2, math.min(param1._3, param2._3), math.min(param1._4, param2._4), math.max(param1._5, param2._5), math.max(param1._6, param2._6)))
      else
        (leftDS_MaxRowSize, leftDS_TotalRowCount, leftDS_MBR_Left, leftDS_MBR_Bottom, leftDS_MBR_Right, leftDS_MBR_Top)

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>Right DS info time in %,d MS".format(System.currentTimeMillis - startTime))
    Helper.loggerSLf4J(debugMode, SparkKNN, ">>rightDS_MaxRowSize:%d\trightDS_TotalRowCount:%d\trightDS_MBR_Left:%.8f\trightDS_MBR_Bottom:%.8f\trightDS_MBR_Right:%.8f\trightDS_MBR_Top:%.8f".format(rightDS_MaxRowSize, rightDS_TotalRowCount, rightDS_MBR_Left, rightDS_MBR_Bottom, rightDS_MBR_Right, rightDS_MBR_Top))

    startTime = System.currentTimeMillis

    val partObjCapacity = computeCapacity(rddRight, rightDS_MaxRowSize, rightDS_TotalRowCount)

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>partObjCapacity time in %,d MS".format(System.currentTimeMillis - startTime))
    Helper.loggerSLf4J(debugMode, SparkKNN, ">>partObjCapacity:%d".format(partObjCapacity))
    //        partObjCapacity = 57702
    //        println(">>" + partObjCapacity)

    val bvGridOp = rddRight.context.broadcast(new GridOperation(rightDS_MBR_Left, rightDS_MBR_Bottom, rightDS_MBR_Right, rightDS_MBR_Top, rightDS_TotalRowCount, k))

    startTime = System.currentTimeMillis

    var lstRangeInfo = ListBuffer[RangeInfo]()
    var rangeInfo: RangeInfo = null

    rddRight
      .mapPartitions(_.map(point => (bvGridOp.value.computeSquareXY(point.x, point.y), 1))) // grid assignment
      .reduceByKey(_ + _) // summarize
      .collect
      .sorted // sorts by column then row ((0, 0), (0, 1), ..., (1, 0), (1, 1) ...)
      .foreach(row => { // group cells on partitions

        if (rangeInfo == null || rangeInfo.totalWeight + row._2 > partObjCapacity) {

          rangeInfo = RangeInfo(row)
          lstRangeInfo.append(rangeInfo)
        }

        rangeInfo.lstMBRCoords.append(row)
        rangeInfo.totalWeight += row._2
        rangeInfo.rightObj = row

        if (row._1._2 < rangeInfo.bottomObj._1._2) rangeInfo.bottomObj = row
        else if (row._1._2 > rangeInfo.topObj._1._2) rangeInfo.topObj = row
      })

    rangeInfo = null

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>rangeInfo time in %,d MS".format(System.currentTimeMillis - startTime))

    //    lstRangeInfo.foreach(rangeInfo =>
    //      Helper.loggerSLf4J(debugMode, SparkKNN, ">>rangeInfo:%s".format(rangeInfo)))

    //    lstRangeInfo.foreach(rInf => println(">1>\t%d\t%d\t%d\t%d\t%d".format(rInf.assignedPartition, rInf.left, rInf.bottom, rInf.right, rInf.top)))

    val globalIndex: SpatialIndex = typeSpatialIndex match {
      case qt if qt == TypeSpatialIndex.quadTree =>
        new QuadTree(buildQTBoundary(bvGridOp.value.computeSquareXY(rightDS_MBR_Left, rightDS_MBR_Bottom), bvGridOp.value.computeSquareXY(rightDS_MBR_Right, rightDS_MBR_Top)))
      case kdt if kdt == TypeSpatialIndex.kdTree =>
        new KdTree()
    }

    val bvArrMBR = rddRight.sparkContext.broadcast(lstRangeInfo.map(_.mbr).toArray)

    startTime = System.currentTimeMillis

    var actualPartitionCount = -1

    globalIndex.insert(lstRangeInfo.map(rangeInfo => {

      actualPartitionCount += 1

      rangeInfo.lstMBRCoords.map(row => new Point(row._1._1, row._1._2, GlobalIndexPointData(row._2, actualPartitionCount)))
    })
      .flatMap(_.seq)
      .iterator)

    val bvGlobalIndex = rddRight.sparkContext.broadcast(globalIndex)

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>GlobalIndex insert time in %,d MS. Points: %s".format(System.currentTimeMillis - startTime, globalIndex))

    lstRangeInfo = null

    val rddSpIdx = rddRight
      .mapPartitions(_.map(point =>
        (bvGlobalIndex.value.findExact(bvGridOp.value.computeSquareXY(point.x, point.y)).userData match {
          case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
        }, point)))
      .partitionBy(new Partitioner() { // group points

        override def numPartitions: Int = actualPartitionCount + 1

        override def getPartition(key: Any): Int = key match {
          case pIdx: Int => pIdx // also used when partitioning rddPoint
        }
      })
      .mapPartitionsWithIndex((pIdx, iter) => { // build spatial index

        val startTime = System.currentTimeMillis

        val spatialIndex: SpatialIndex = typeSpatialIndex match {
          case qt if qt == TypeSpatialIndex.quadTree => new QuadTree(buildQTBoundary(bvArrMBR.value(pIdx), bvGridOp.value.squareLen))
          case kdt if kdt == TypeSpatialIndex.kdTree => new KdTree()
        }

        //        if (pIdx == 7 || pIdx == 1 || pIdx == 0)
        //          println(pIdx)

        spatialIndex.insert(iter.map(_._2))

        Helper.loggerSLf4J(debugMode, SparkKNN, ">>SpatialIndex on partition %d time in %,d MS. Points: %s".format(pIdx, System.currentTimeMillis - startTime, spatialIndex))

        val any: Any = spatialIndex

        Iterator((pIdx, any))
      }, preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_ONLY)

    //    lstRangeInfo.foreach(row => println(">2>\t%s".format(row)))
    //    println(">2>=====================================")

    startTime = System.currentTimeMillis

    val numRounds = if (isAllJoin) {

      val (numR, (maxRowSize, totalRowCount, mbrLeft, mbrBott, mbrRight, mbrTop)) =
        rddLeft
          .mapPartitions(_.map(point => (bvGridOp.value.computeSquareXY(point.x, point.y), (point.userData.toString.length, 1L, point.x, point.y, point.x, point.y))))
          .reduceByKey((param1, param2) =>
            (math.max(param1._1, param2._1), param1._2 + param2._2, math.min(param1._3, param2._3), math.min(param1._4, param2._4), math.max(param1._5, param2._5), math.max(param1._6, param2._6)))
          .mapPartitions(iter =>
            Iterator(iter.map(row => (SpatialIdxRangeLookup.getLstPartition(bvGlobalIndex.value, row._1, k).size, row._2))
              .reduce((param1, param2) =>
                (math.max(param1._1, param2._1), (math.max(param1._2._1, param2._2._1), param1._2._2 + param2._2._2, math.min(param1._2._3, param2._2._3), math.min(param1._2._4, param2._2._4), math.max(param1._2._5, param2._2._5), math.max(param1._2._6, param2._2._6)))))
          )
          .reduce((param1, param2) =>
            (math.max(param1._1, param2._1), (math.max(param1._2._1, param2._2._1), param1._2._2 + param2._2._2, math.min(param1._2._3, param2._2._3), math.min(param1._2._4, param2._2._4), math.max(param1._2._5, param2._2._5), math.max(param1._2._6, param2._2._6))))

      leftDS_MaxRowSize = maxRowSize
      leftDS_TotalRowCount = totalRowCount
      leftDS_MBR_Left = mbrLeft
      leftDS_MBR_Bottom = mbrBott
      leftDS_MBR_Right = mbrRight
      leftDS_MBR_Top = mbrTop

      numR
    }
    else
      rddLeft
        .mapPartitions(_.map(point => bvGridOp.value.computeSquareXY(point.x, point.y)).toSet.iterator.map((row: (Double, Double)) => (row, Byte.MinValue)))
        .reduceByKey((x, _) => x)
        .mapPartitions(iter => Iterator(iter.map(row => SpatialIdxRangeLookup.getLstPartition(bvGlobalIndex.value, row._1, k).size).max))
        .max

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>LeftDS numRounds time in %,d MS, numRounds: %d".format(System.currentTimeMillis - startTime, numRounds))

    var rddPoint = rddLeft
      .mapPartitions(iter => iter.map(point => {

//        if (point.userData.toString().equalsIgnoreCase("bread_3_a_822279"))
//          println

        val lstPartitionId = SpatialIdxRangeLookup.getLstPartition(bvGlobalIndex.value, bvGridOp.value.computeSquareXY(point.x, point.y), k)

        val rowData: Any = new RowData(point, SortedList[Point](k), Random.shuffle(lstPartitionId.tail))

        // println(">>>" + pidx + "\t" + (System.currentTimeMillis-start))

        (lstPartitionId.head, rowData)
      }))

    (0 until numRounds).foreach(iterationNum => {
      rddPoint = rddSpIdx
        .union(new ShuffledRDD(rddPoint, rddSpIdx.partitioner.get))
        .mapPartitionsWithIndex((pIdx, iter) => {

          val start = System.currentTimeMillis()

          val spatialIndex = iter.next._2 match {
            case spIdx: SpatialIndex => spIdx
          }

          val l = iter.map(row => {

            //            if (pIdx == 7 || pIdx == 1 || pIdx == 0)
            //              println(iterationNum)

            row._2 match {
              case rowPoint: RowData =>

                if (rowPoint.lstPartitionId == null)
                  row
                else {

                  //                  if (rowPoint.point.userData.toString().equalsIgnoreCase("bread_3_a_822279"))
                  //                    println

                  spatialIndex.nearestNeighbor(rowPoint.point, rowPoint.sortedList)

                  val nextPIdx = if (rowPoint.lstPartitionId.isEmpty) row._1 else rowPoint.lstPartitionId.head
                  rowPoint.lstPartitionId = if (rowPoint.lstPartitionId.isEmpty) null else /*Random.shuffle(*/ rowPoint.lstPartitionId.tail

                  (nextPIdx, rowPoint)
                }
            }
          })

          Helper.loggerSLf4J(debugMode, SparkKNN, ">>>Iteration#: %d Partition %d done in %d".format(iterationNum, pIdx, System.currentTimeMillis() - start))

          l
        })

      bvArrMBR.unpersist()
    })

    //    println(">>>" + rddPoint.getNumPartitions)

    //    rddPoint.foreach(println)

    rddPoint.map(_._2 match {
      case rowData: RowData => (rowData.point, rowData.sortedList.map(nd => (nd.distance, nd.data)))
    })
    //
    //    null
  }

  private def computeCapacity(rddRight: RDD[Point], maxRowSize: Int, totalRowCount: Long) = {

    // 7% reduction in memory to account for overhead operations
    val execAvailableMemory = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory", rddRight.context.getExecutorMemoryStatus.map(_._2._1).max + "B")) // skips memory of core assigned for Hadoop daemon
    val exeOverheadMemory = math.max(384, 0.1 * execAvailableMemory).toLong + 1
    val coresPerExec = rddRight.context.getConf.getInt("spark.executor.cores", 1)
    val coreAvailableMemory = (execAvailableMemory - exeOverheadMemory - exeOverheadMemory) / coresPerExec
    // deduct yarn overhead

    val pointDummy = new Point(0, 0, Array.fill[Char](maxRowSize)(' ').mkString(""))
    val sortSetDummy = SortedList[Point](k)
    val lstPartitionIdDummy = List.fill[Int](rddRight.getNumPartitions)(0)
    val rowData = new RowData(pointDummy, sortSetDummy, lstPartitionIdDummy)
    val spatialIndexDummy = typeSpatialIndex match {
      case qt if qt == TypeSpatialIndex.quadTree =>
        new QuadTree(Rectangle(new Geom2D(0, 0), new Geom2D(0, 0)))
      case kdt if kdt == TypeSpatialIndex.kdTree =>
        new KdTree()
    }

    val pointCost = SizeEstimator.estimate(pointDummy)
    val rowDataCost = SizeEstimator.estimate(rowData) + (pointCost * k)
    val quadTreeCost = SizeEstimator.estimate(spatialIndexDummy)

    // exec mem cost = 1QT + 1Pt and matches
    val partObjCapacity = ((coreAvailableMemory - quadTreeCost - rowDataCost) / pointCost).toInt

    var numParts = (totalRowCount / partObjCapacity).toInt + 1 //, (rddRight.context.getExecutorMemoryStatus.size - 1) * coresPerExec)
    if (numParts <= 1) numParts = rddRight.getNumPartitions

    Helper.loggerSLf4J(debugMode, SparkKNN, ">>execAvailableMemory=%d\texeOverheadMemory=%d\tcoresPerExec=%d\tcoreAvailableMemory=%d\tpointCost=%d\trowDataCost=%d\tquadTreeCost=%d\tpartObjCapacity=%d\tnumParts=%d".format(execAvailableMemory, exeOverheadMemory, coresPerExec, coreAvailableMemory, pointCost, rowDataCost, quadTreeCost, partObjCapacity, numParts))

    (totalRowCount / numParts).toInt + 1
  }

  private def buildQTBoundary(mbrMin: (Double, Double), mbrMax: (Double, Double)): Rectangle = {

    val halfXY = new Geom2D(((mbrMax._1 - mbrMin._1) + 1) / 2, ((mbrMax._2 - mbrMin._2) + 1) / 2)

    Rectangle(new Geom2D(mbrMin._1 + halfXY.x, mbrMin._2 + halfXY.y), halfXY)
  }

  private def buildQTBoundary(mbr: (Double, Double, Double, Double), gridSquareLen: Double): Rectangle = {

    val minX = mbr._1 * gridSquareLen
    val minY = mbr._2 * gridSquareLen
    val maxX = mbr._3 * gridSquareLen + gridSquareLen
    val maxY = mbr._4 * gridSquareLen + gridSquareLen

    val halfWidth = (maxX - minX) / 2
    val halfHeight = (maxY - minY) / 2

    Rectangle(new Geom2D(halfWidth + minX, halfHeight + minY), new Geom2D(halfWidth, halfHeight))
  }
}