package org.cusp.bdi.sknn

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.util.{SpIndexInfo, SpIndexOperations}
import org.cusp.bdi.ds.{Box, Point, qt}
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.{Helper, SortedList}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

case class PartitionInfo(left: (Double, Double), bottom: (Double, Double), right: (Double, Double), top: (Double, Double), uniqueIdentifier: Int, totalPoints: Long) {

  var assignedPart: Int = -1

  override def toString: String =
    "%s\t%s\t%s\t%s\t%d\t%d\t%d".format(left.toString, bottom.toString, right.toString, top.toString, assignedPart, uniqueIdentifier, totalPoints)
}

object SparkKNN {

  def getSparkKNNClasses: Array[Class[_]] =
    Array(classOf[QuadTree],
      classOf[SpIndexInfo],
      classOf[GridOperation],
      SpIndexOperations.getClass,
      Helper.getClass,
      classOf[SpIndexInfo],
      classOf[SortedList[_]],
      classOf[Box],
      classOf[Point])
}

case class SparkKNN(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int) {

  // for testing, remove ...
  var minPartitions = 0

  def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] =
    knnJoinExecute(rddLeft, rddRight, k).union(knnJoinExecute(rddRight, rddLeft, k))

  def knnJoin() =
    knnJoinExecute(rddLeft, rddRight, k)

  private def knnJoinExecute(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int): RDD[(Point, Iterable[(Double, Point)])] /*: RDD[(Point, Iterable[(Double, Point)])]*/ = {

    var (execRowCapacity, totalRowCount) = computeCapacity(rddRight, k)

    //    execRowCapacity = 58821
    //                println(">>" + execRowCapacity)

    var arrPartRangeCount = computePartitionRanges(rddRight, execRowCapacity)

    val arrPartInf = rddRight
      .mapPartitions(_.map(point => (point.x, point.y)))
      .repartitionAndSortWithinPartitions(new Partitioner() {

        // places in containers along the x-axis and sort by x-coord
        override def numPartitions: Int = arrPartRangeCount.length

        override def getPartition(key: Any): Int =
          key match {
            case xCoord: Double =>
              binarySearchArr(arrPartRangeCount, xCoord.toLong)
          }
      })
      .mapPartitions(iter => {

        val lstPartitionRangeCount = ListBuffer[PartitionInfo]()

        var left = iter.next
        var bottom = left
        var right = left
        var top = left

        var count = 1

        do {
          if (count == execRowCapacity || !iter.hasNext) {

            lstPartitionRangeCount.append(PartitionInfo(left, bottom, right, top, Random.nextInt(), count))

            if (iter.hasNext) {

              left = iter.next
              bottom = left
              right = left
              top = left
              count = 1
            }
            else
              count = 0
          }
          else if (iter.hasNext) {

            right = iter.next
            count += 1

            if (right._2 < bottom._2) bottom = right
            else if (right._2 > top._2) top = right
          }
          else
            count = 0
        } while (count != 0)

        lstPartitionRangeCount.iterator
      })
      .sortBy(_.left._1)
      .collect

    arrPartRangeCount = null

    val actualPartitionCount = AssignToPartitions(arrPartInf, execRowCapacity).getPartitionCount

    val mapUIdPartId = arrPartInf.map(partInf => (partInf.uniqueIdentifier, partInf)).toMap

    val mbrDS1 = arrPartInf.map(partInf => (partInf.left._1, partInf.bottom._2, partInf.right._1, partInf.top._2))
      .fold((Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))((mbr1, mbr2) => (math.min(mbr1._1, mbr2._1), math.min(mbr1._2, mbr2._2), math.max(mbr1._3, mbr2._3), math.max(mbr1._4, mbr2._4)))

    //    arrPartInf.foreach(pInf => println(">1>\t%s\t%s\t%s\t%s\t%d\t%d\t%d".format(pInf.left._1, pInf.bottom._2, pInf.right._1, pInf.top._2, pInf.totalPoints, pInf.assignedPart, pInf.uniqueIdentifier)))

    val gridOp = GridOperation(mbrDS1, totalRowCount, k)

    val rddSpIdx = rddRight
      .mapPartitions(_.map(point => (binarySearchPartInf(arrPartInf, point.x).uniqueIdentifier, point)))
      .partitionBy(new Partitioner() {
        override def numPartitions: Int = actualPartitionCount

        override def getPartition(key: Any): Int = key match {
          case uId: Int => mapUIdPartId(uId).assignedPart
        }
      })
      .mapPartitions(iter => {

        val mapSpIdx = mutable.HashMap[Int, SpIndexInfo]()

        iter.foreach(row => {

          //          if (row._2.userData.toString().equalsIgnoreCase("Taxi_2_A_295759"))
          //            print("")

          val partInf = mapUIdPartId(row._1) // binarySearchPartInf(arrPartInf, row._2.x)

          mapSpIdx.getOrElse(partInf.uniqueIdentifier, {

            val minX = partInf.left._1.toLong
            val minY = partInf.bottom._2.toLong
            val maxX = partInf.right._1.toLong + 1
            val maxY = partInf.top._2.toLong + 1

            val halfWidth = (maxX - minX) / 2.0
            val halfHeight = (maxY - minY) / 2.0

            val newIdx = SpIndexInfo(qt.QuadTree(Box(new Point(halfWidth + minX, halfHeight + minY), new Point(halfWidth, halfHeight))))
            newIdx.uniqueIdentifier = partInf.uniqueIdentifier

            mapSpIdx += (partInf.uniqueIdentifier -> newIdx)

            newIdx
          })
            .dataStruct.insert(row._2)
        })

        mapSpIdx.valuesIterator
      } /*, preservesPartitioning = true*/)
      .persist(StorageLevel.MEMORY_ONLY)

    //        println(">2>=====================================")
    //        rddSpIdx.foreach(qtInf => println(">2>\t%d%s%n".format(mapUIdPartId(qtInf.uniqueIdentifier).assignedPart, qtInf.toString())))
    //        println(">2>=====================================")

    // (box#, Count)
    val arrGridAndSpIdxInf = rddSpIdx
      .mapPartitions(iter => {

        //        val object GridOperation = new GridOperation(mbrDS1, totalRowCount, k)

        iter.map(qtInf => qtInf.dataStruct
          .getAllPoints
          .iterator
          .map(_.map(point => (gridOp.computeBoxXY(point.x, point.y), qtInf.uniqueIdentifier))))
          .flatMap(_.seq)
          .flatMap(_.seq)
          .map(row => (row._1, Set(row._2)))
      })
      .reduceByKey(_ ++ _)
      .collect

    //    arrGridAndSpIdxInf.foreach(row => println(">3>\t%d\t%d\t%d\t%s".format(row._1._1, row._1._2, row._2._1, row._2._2.mkString(","))))
    //    arrPartInf = null
    //    println(">>" + gridOp.getBoxWH)

    val leftBot = gridOp.computeBoxXY(mbrDS1._1, mbrDS1._2)
    val rightTop = gridOp.computeBoxXY(mbrDS1._3, mbrDS1._4)

    val halfWidth = ((rightTop._1 - leftBot._1) + 1) / 2.0
    val halfHeight = ((rightTop._2 - leftBot._2) + 1) / 2.0

    val globalIndex = new QuadTree(Box(new Point(halfWidth + leftBot._1, halfHeight + leftBot._2), new Point(halfWidth, halfHeight)))

    arrGridAndSpIdxInf.foreach(row => globalIndex.insert(new Point(row._1._1, row._1._2, row._2)))

    val bvQTGlobalIndex = rddLeft.context.broadcast(globalIndex)

    var rddPoint = rddLeft
      .mapPartitions(iter => {

        //        val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

        iter.map(point => {

          //                              if (point.userData.toString().equalsIgnoreCase("bread_1_b_524110"))
          //                                println

          val lstUId = SpIndexOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k, gridOp.getErrorRange)
            .toList

          //          if (lstUId.size >= 10)
          //            println(QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k))

          val tuple: Any = (point, SortedList[Point](k, allowDuplicates = false), lstUId)

          (mapUIdPartId(lstUId.head).assignedPart, tuple)
        })
      })

    //    println(">>" + rddPoint.mapPartitions(_.map(_._2.asInstanceOf[(Point, SortedList[Point], List[Int])]._3.size))
    //      .max())

    val numRounds = rddLeft
      .mapPartitions(iter => {

        //        val gridOp = new GridOperation(mbrDS1, totalRowCount, k)
        val b = 0.toByte

        iter.map(point => (gridOp.computeBoxXY(point.x, point.y), b))
      })
      .reduceByKey((x, _) => x)
      .mapPartitions(iter => Iterator(iter.map(row => SpIndexOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, row._1, k, gridOp.getErrorRange).map(mapUIdPartId(_).assignedPart).size).max))
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

          val lstPartQT = ListBuffer[SpIndexInfo]()

          iter.map(row =>
            row._2 match {

              case qtInf: SpIndexInfo =>

                lstPartQT += qtInf

                null
              case _ =>

                val (point, sortSetSqDist, lstUId) = row._2.asInstanceOf[(Point, SortedList[Point], List[Int])]

                if (lstUId == null)
                  row
                else {

                  //  if ((pIdx == 16 || pIdx == 8) && point.userData.toString().equalsIgnoreCase("taxi_2_b_731083"))
                  //    println(pIdx)

                  // build a list of QT to check
                  val lstVisitQTInf = lstPartQT.filter(partQT => lstUId.contains(partQT.uniqueIdentifier))

                  //  if (point.userData.toString().equalsIgnoreCase("Bread_3_B_199024"))
                  //    println(">>rn>>"+roundNum)

                  SpIndexOperations.nearestNeighbor(lstVisitQTInf, point, sortSetSqDist, k)

                  // randomize order to lessen query skews
                  val setLeftQTInf = /*Random.shuffle(*/ lstUId.filterNot(lstVisitQTInf.map(_.uniqueIdentifier).contains _) /*)*/

                  // send done rows to a random partition to lessen query skews.
                  if (setLeftQTInf.isEmpty)
                    (Random.nextInt(actualPartitionCount), (point, sortSetSqDist, null))
                  else
                    (mapUIdPartId(setLeftQTInf.head).assignedPart, (point, sortSetSqDist, setLeftQTInf))
                }
            }
          )
            .filter(_ != null)
        })
    })

    rddPoint.mapPartitions(_.map(row => {

      val (point, sortSetSqDist, _) = row._2.asInstanceOf[(Point, SortedList[Point], _)]

      (point, sortSetSqDist.map(nd => (nd.distance, nd.data)))
    }))
  }

  private def computeCapacity(rddRight: RDD[Point], k: Int): (Int, Long) = {

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
    val dataStructEmptyDummy = SpIndexInfo(qt.QuadTree(Box(new Point(pointDummy), new Point(pointDummy))))
    val sortSetDummy = SortedList[String](k, allowDuplicates = false)

    val pointCost = SizeEstimator.estimate(pointDummy)
    val sortSetCost = /* pointCost + */ SizeEstimator.estimate(sortSetDummy) + (k * pointCost)
    val dataStructCost = SizeEstimator.estimate(dataStructEmptyDummy)

    // exec mem cost = 1QT + 1Pt and matches
    var execRowCapacity = ((execAvailableMemory - exeOverheadMemory - dataStructCost) / pointCost).toInt

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

    //    lstPartRangeCount.foreach(row => println(">4>\t%.8f\t%.8f\t%d".format(row._1, row._2, row._3)))

    lstPartRangeCount.toArray
  }

  private def binarySearchArr(arrHorizDist: Array[(Double, Double)], pointX: Long): Int = {

    var topIdx = 0
    var botIdx = arrHorizDist.length - 1

    if (pointX < arrHorizDist.head._1)
      return 0
    else if (pointX > arrHorizDist.last._1)
      return arrHorizDist.length - 1
    else
      while (botIdx >= topIdx) {

        val midIdx = (topIdx + botIdx) / 2
        val midRegion = arrHorizDist(midIdx)

        if (pointX >= midRegion._1 && pointX <= midRegion._2)
          return midIdx
        else if (pointX < midRegion._1)
          botIdx = midIdx - 1
        else
          topIdx = midIdx + 1
      }

    throw new Exception("binarySearchArr() for %,d failed in horizontal distribution %s".format(pointX, arrHorizDist.mkString("Array(", ", ", ")")))
  }

  private def binarySearchPartInf(arrPartInf: Array[PartitionInfo], pointX: Double): PartitionInfo = {

    var topIdx = 0
    var botIdx = arrPartInf.length - 1

    if (pointX < arrPartInf.head.left._1)
      arrPartInf.head
    if (pointX > arrPartInf.last.right._1)
      arrPartInf.last
    else {

      var midIdx = -1
      var bestEstimate: PartitionInfo = null

      while (botIdx >= topIdx) {

        midIdx = (topIdx + botIdx) / 2
        val midRegion = arrPartInf(midIdx)

        if (pointX >= midRegion.left._1) {
          if (pointX <= midRegion.right._1)
            return midRegion
          else
            bestEstimate = midRegion
        }

        if (pointX < midRegion.left._1)
          botIdx = midIdx - 1
        else
          topIdx = midIdx + 1
      }

      bestEstimate
    }
  }
}