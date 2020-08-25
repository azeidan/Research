package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.qt.{QuadTree, QuadTreeOperations}
import org.cusp.bdi.ds.{Box, Point}
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.{Helper, Node, SortedList}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

case class PartitionInfo(uniqueIdentifier: Int) {

  var totalPoints = 0L
  var assignedPart: Int = -1
  var left = 0.0
  var bottom = 0.0
  var right = 0.0
  var top = 0.0

  override def toString: String =
    "%d\t%d\t%d".format(assignedPart, uniqueIdentifier, totalPoints)
}

class RowData(_point: Point, _sortedList: SortedList[Point], _lstQuadTreeUId: List[Int]) {

  val point = _point
  val sortedList = _sortedList
  var lstQuadTreeUId = _lstQuadTreeUId
}

case class GlobalIndexPointData(numPoints: Long, setUId: Set[Int]) extends Serializable with Comparable[GlobalIndexPointData] {
  override def compareTo(other: GlobalIndexPointData) = 1
}

object SparkKNN {

  def getSparkKNNClasses: Array[Class[_]] =
    Array(classOf[PartitionInfo],
      classOf[RowData],
      classOf[GlobalIndexPointData],
      classOf[QuadTree],
      QuadTreeOperations.getClass,
      classOf[GridOperation],
      classOf[QuadTreeInfo],
      Helper.getClass,
      classOf[Node[_]],
      classOf[SortedList[_]],
      classOf[Box],
      classOf[Point],
      classOf[AssignToPartitions],
      RDD_Store.getClass,
      SparkKNN_Arguments.getClass)
}

case class SparkKNN(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int) {

  // for testing, remove ...
  var minPartitions = 0

  def knnJoin() =
    knnJoinExecute(rddLeft, rddRight)

  def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] =
    knnJoinExecute(rddLeft, rddRight)
      .union(knnJoinExecute(rddRight, rddLeft))

  private def knnJoinExecute(rddLeft: RDD[Point], rddRight: RDD[Point]): RDD[(Point, Iterable[(Double, Point)])] = {

    val (execRowCapacity, totalRowCount) = computeCapacity(rddRight)

    //        execRowCapacity = 57702
    //        println(">>" + execRowCapacity)

    var arrPartRange = computePartitionRanges(rddRight, execRowCapacity)

    var arrPartInf = rddRight.mapPartitions(_.map(point => (point.x, point.y)))
      .repartitionAndSortWithinPartitions(new Partitioner() {

        // places in containers along the x-axis and sort by x-coord
        override def numPartitions: Int = arrPartRange.length

        override def getPartition(key: Any): Int =
          key match {
            case xCoord: Double =>
              binarySearchArrPartRange(arrPartRange, xCoord.toLong)
          }
      })
      .mapPartitions(iter => {

        val lstPartitionRangeCount = ListBuffer[PartitionInfo]()
        var partInf = PartitionInfo(Random.nextInt())

        lstPartitionRangeCount.append(partInf)

        while (iter.hasNext) {

          val row = iter.next

          if (partInf.totalPoints == execRowCapacity) {

            partInf = PartitionInfo(Random.nextInt())
            lstPartitionRangeCount.append(partInf)
          }

          partInf.totalPoints += 1

          if (partInf.totalPoints == 1) {

            partInf.left = row._1
            partInf.bottom = row._2
            partInf.right = partInf.left
            partInf.top = partInf.bottom
          }
          else {

            partInf.right = row._1

            if (row._2 < partInf.bottom) partInf.bottom = row._2
            else if (row._2 > partInf.top) partInf.top = row._2
          }
        }

        lstPartitionRangeCount.iterator
      })
      .sortBy(_.left)
      .collect

    arrPartRange = null

    val actualPartitionCount = AssignToPartitions(arrPartInf, execRowCapacity).getPartitionCount

    val mapUIdPartId = arrPartInf.map(partInf => (partInf.uniqueIdentifier, partInf)).toMap

    //    arrPartInf.foreach(pInf => println(">1>\t%s\t%s\t%s\t%s\t%d\t%d\t%d".format(pInf.left, pInf.bottom, pInf.right, pInf.top, pInf.totalPoints, pInf.assignedPart, pInf.uniqueIdentifier)))

    val rddSpIdx = rddRight
      .mapPartitions(_.map(point => (binarySearchPartInf(arrPartInf, point.x).uniqueIdentifier, point)))
      .partitionBy(new Partitioner() {
        override def numPartitions: Int = actualPartitionCount

        override def getPartition(key: Any): Int = key match {
          case uId: Int => mapUIdPartId(uId).assignedPart
        }
      })
      .mapPartitions(iter => {

        val mapSpIdx = mutable.HashMap[Int, QuadTreeInfo]()

        iter.foreach(row => {

          //          if (row._2.userData.toString().equalsIgnoreCase("Taxi_2_A_295759"))
          //            print("")

          val partInf = mapUIdPartId(row._1) // binarySearchPartInf(arrPartInf, row._2.x)

          mapSpIdx.getOrElse(partInf.uniqueIdentifier, {

            val minX = partInf.left.toLong
            val minY = partInf.bottom.toLong
            val maxX = partInf.right.toLong + 1
            val maxY = partInf.top.toLong + 1

            val halfWidth = (maxX - minX) / 2.0
            val halfHeight = (maxY - minY) / 2.0

            val newIdx = new QuadTreeInfo(Box(new Point(halfWidth + minX, halfHeight + minY), new Point(halfWidth, halfHeight)))
            newIdx.uniqueIdentifier = partInf.uniqueIdentifier

            mapSpIdx += (partInf.uniqueIdentifier -> newIdx)

            newIdx
          })
            .quadTree.insert(row._2)
        })

        mapSpIdx.valuesIterator
      } /*, preservesPartitioning = true*/)
      .persist(StorageLevel.MEMORY_ONLY)

    //    println(">2>=====================================")
    //    rddSpIdx.foreach(qtInf => println(">2>\t%d%s%n".format(mapUIdPartId(qtInf.uniqueIdentifier).assignedPart, qtInf.toString())))
    //    println(">2>=====================================")

    var mbrDS1 = arrPartInf.map(partInf => (partInf.left, partInf.bottom, partInf.right, partInf.top))
      .fold((Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))((mbr1, mbr2) => (math.min(mbr1._1, mbr2._1), math.min(mbr1._2, mbr2._2), math.max(mbr1._3, mbr2._3), math.max(mbr1._4, mbr2._4)))

    val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

    val leftBot = gridOp.computeBoxXY(mbrDS1._1, mbrDS1._2)
    val rightTop = gridOp.computeBoxXY(mbrDS1._3, mbrDS1._4)

    mbrDS1 = null

    val halfWidth = ((rightTop._1 - leftBot._1) + 1) / 2.0
    val halfHeight = ((rightTop._2 - leftBot._2) + 1) / 2.0

    val globalIndex = new QuadTree(Box(new Point(halfWidth + leftBot._1, halfHeight + leftBot._2), new Point(halfWidth, halfHeight)))

    val bvQTGlobalIndex = rddLeft.context.broadcast(globalIndex)

    // (box#, Count)
    rddSpIdx
      .mapPartitions(_.map(qtInf => qtInf.quadTree
        .getAllPoints
        .iterator
        .map(_.map(point => (gridOp.computeBoxXY(point.x, point.y), (1L, Set(qtInf.uniqueIdentifier))))))
        .flatMap(_.seq)
        .flatMap(_.seq)
      )
      .reduceByKey((x, y) => (x._1 + y._1, x._2 ++ y._2))
      .collect
      .foreach(row => globalIndex.insert(new Point(row._1._1, row._1._2, GlobalIndexPointData(row._2._1, row._2._2))))

    //    arrGridAndSpIdxInf.foreach(row => println(">3>\t%d\t%d\t%d\t%s".format(row._1._1, row._1._2, row._2._1, row._2._2.mkString(","))))

    arrPartInf = null

    var rddPoint = rddLeft
      .mapPartitions(iter => {

        iter.map(point => {

          //          if (point.userData.toString().equalsIgnoreCase("bus_1_b_409423"))
          //            println

          val lstUId = QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k, gridOp.getErrorRange)
            .toList

          val rowPointData: Any = new RowData(point, SortedList[Point](k, allowDuplicates = false), lstUId)

          (mapUIdPartId(lstUId.head).assignedPart, rowPointData)
        })
      })

    //    println(rddPoint.map(_._2.asInstanceOf[(Point, SortedList[Point], List[Int])]._3.size).max)

    val numRounds = rddLeft
      .mapPartitions(iter => {

        val b = 0.toByte

        iter.map(point => (gridOp.computeBoxXY(point.x, point.y), b))
      })
      .reduceByKey((x, _) => x)
      .mapPartitions(iter => {

        Iterator(iter.map(row => {

          //          if (QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, row._1, k, gridOp.getErrorRange).map(mapUIdPartId(_).assignedPart).size >= 8)
          //            println(QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, row._1, k, gridOp.getErrorRange).map(mapUIdPartId(_).assignedPart).size)

          QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, row._1, k, gridOp.getErrorRange).map(mapUIdPartId(_).assignedPart).size
        }).max)
      })
      .max

    (0 until numRounds).foreach(_ => {

      rddPoint = rddSpIdx
        .mapPartitions(_.map(qtInf => {

          val tuple: Any = qtInf

          (mapUIdPartId(qtInf.uniqueIdentifier).assignedPart, tuple)
        }) /*, true*/)
        .union(rddPoint)
        .partitionBy(new Partitioner() {

          override def numPartitions: Int = actualPartitionCount

          override def getPartition(key: Any): Int =
            key match {
              case partId: Int => partId
            }
        })
        .mapPartitions(iter => {

          val lstPartQT = ListBuffer[QuadTreeInfo]()

          iter.map(row => {

            row._2 match {

              case qtInf: QuadTreeInfo =>

                lstPartQT += qtInf

                null
              case rowPointData: RowData =>

                if (rowPointData.lstQuadTreeUId == null)
                  row
                else {

                  // build a list of QT to check
                  val lstVisitQTInf = lstPartQT.filter(partQT => rowPointData.lstQuadTreeUId.contains(partQT.uniqueIdentifier))

                  QuadTreeOperations.nearestNeighbor(lstVisitQTInf, rowPointData.point, rowPointData.sortedList, k)

                  // randomize order to lessen query skews
                  val lstLeftQTInf = Random.shuffle(rowPointData.lstQuadTreeUId.filterNot(lstVisitQTInf.map(_.uniqueIdentifier).contains _))

                  rowPointData.lstQuadTreeUId = if (lstLeftQTInf.isEmpty) null else lstLeftQTInf

                  (if (lstLeftQTInf.isEmpty) row._1 else mapUIdPartId(lstLeftQTInf.head).assignedPart, rowPointData)

                  //                  if (lstLeftQTInf.isEmpty)
                  //                    (row._1, (point, sortSetSqDist, null))
                  //                  else
                  //                    (mapUIdPartId(lstLeftQTInf.head).assignedPart, (point, sortSetSqDist, lstLeftQTInf))
                }
            }
          })
            .filter(_ != null)
        })
    })

    rddPoint.mapPartitions(_.map(_._2 match { case rowPointData: RowData => rowPointData })
      .map(rowPointData => (rowPointData.point, rowPointData.sortedList.map(nd => (nd.distance, nd.data)))))
  }

  private def binarySearchArrPartRange(arrPartRange: => Array[(Double, Double)], pointX: => Long): Int = {

    var topIdx = 0
    var botIdx = arrPartRange.length - 1

    while (botIdx >= topIdx) {

      val midIdx = (topIdx + botIdx) / 2
      val midRegion = arrPartRange(midIdx)

      if (pointX >= midRegion._1 && pointX <= midRegion._2)
        return midIdx
      else if (pointX < midRegion._1)
        botIdx = midIdx - 1
      else
        topIdx = midIdx + 1
    }

    throw new Exception("binarySearchArrPartRange() for %,d failed in horizontal distribution %s".format(pointX, arrPartRange.mkString("Array(", ", ", ")")))
  }

  private def binarySearchPartInf(arrPartInf: Array[PartitionInfo], pointX: Double): PartitionInfo = {

    var topIdx = 0
    var botIdx = arrPartInf.length - 1

    while (botIdx >= topIdx) {

      val midIdx = (topIdx + botIdx) / 2
      val midRegion = arrPartInf(midIdx)

      if (pointX >= midRegion.left && pointX <= midRegion.right)
        return midRegion
      else if (pointX < midRegion.left)
        botIdx = midIdx - 1
      else
        topIdx = midIdx + 1
    }

    throw new Exception("binarySearchPartInf() for %.8f failed in horizontal distribution %s".format(pointX, arrPartInf.mkString("Array(", ", ", ")")))
  }

  private def computeCapacity(rddRight: RDD[Point]): (Int, Long) = {

    // 7% reduction in memory to account for overhead operations
    val execAvailableMemory = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory", rddRight.context.getExecutorMemoryStatus.map(_._2._1).max + "B")) // skips memory of core assigned for Hadoop daemon
    // deduct yarn overhead
    val exeOverheadMemory = math.max(384, 0.1 * execAvailableMemory).toLong + 1

    val (maxRowSize, totalRowCount) = rddRight.mapPartitions(iter =>
      Iterator(iter.map(point => (point.userData.toString.length, 1L))
        .fold(0, 0L)((param1, param2) => (math.max(param1._1, param2._1), param1._2 + param2._2)))
    )
      .fold((0, 0L))((t1, t2) => (math.max(t1._1, t2._1), t1._2 + t2._2))

    //        val gmGeomDummy = GMPoint((0 until maxRowSize).map(_ => " ").mkString(""), (0, 0))
    val userData = Array.fill[Char](maxRowSize)(' ').mkString("Array(", ", ", ")")
    val pointDummy = new Point(0, 0, userData)
    val quadTreeEmptyDummy = new QuadTreeInfo(Box(new Point(pointDummy), new Point(pointDummy)))
    val sortSetDummy = SortedList[String](k, allowDuplicates = false)

    val pointCost = SizeEstimator.estimate(pointDummy)
    val sortSetCost = /* pointCost + */ SizeEstimator.estimate(sortSetDummy) + (k * pointCost)
    val quadTreeCost = SizeEstimator.estimate(quadTreeEmptyDummy)

    // exec mem cost = 1QT + 1Pt and matches
    var execRowCapacity = ((execAvailableMemory - exeOverheadMemory - quadTreeCost) / pointCost).toInt

    var numParts = (totalRowCount.toDouble / execRowCapacity).toInt + 1

    if (numParts == 1) {

      numParts = rddRight.getNumPartitions
      execRowCapacity = (totalRowCount / numParts).toInt
    }

    (execRowCapacity, totalRowCount)
  }

  private def computePartitionRanges(rddRight: RDD[Point], execRowCapacity: Int) = {

    val arrContainerRangeAndCount = rddRight
      .mapPartitions(_.map(point => ((point.x / execRowCapacity).toLong, 1L)))
      .reduceByKey(_ + _)
      .collect
      .sortBy(_._1) // by bucket #
      .map(row => {

        val start = row._1 * execRowCapacity
        val end = start + execRowCapacity - 1

        (start, end, row._2)
      })

    val lstPartRangeCount = ListBuffer[(Double, Double)]()
    val lastIdx = arrContainerRangeAndCount.length - 1
    var idx = 0
    var row = arrContainerRangeAndCount(idx)
    var start = row._1
    var totalFound = 0L

    while (idx <= lastIdx) {

      val borrowCount = execRowCapacity - totalFound

      if (borrowCount > row._3 && idx != lastIdx) {

        totalFound += row._3
        idx += 1
        row = arrContainerRangeAndCount(idx)
      }
      else {

        val percent = if (borrowCount > row._3.toDouble) 1 else borrowCount / row._3.toDouble

        val end = row._1 + ((row._2 - row._1) * percent).toLong + 1

        lstPartRangeCount.append((start, end))

        if (idx == lastIdx)
          idx += 1
        else {

          totalFound = 0

          if (borrowCount == row._3) {

            idx += 1
            row = arrContainerRangeAndCount(idx)
            start = row._1
          }
          else {

            start = end + 1
            row = (start, row._2, row._3 - borrowCount)
          }
        }
      }
    }

    lstPartRangeCount.toArray
  }
}