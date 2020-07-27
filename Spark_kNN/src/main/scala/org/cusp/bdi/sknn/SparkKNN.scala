package org.cusp.bdi.sknn

import com.insightfullogic.quad_trees.{Box, Point, QuadTree, QuadTreeDigest}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.util.Random

case class PartitionInfo(left: (Double, Double), bottom: (Double, Double), right: (Double, Double), top: (Double, Double), uniqueIdentifier: Int, totalPoints: Long) {

  var assignedPart: Int = -1

  override def toString: String =
    "%d\t%d\t%d".format(assignedPart, uniqueIdentifier, totalPoints)
}

object SparkKNN {

  def getSparkKNNClasses(): Array[Class[_]] =
    Array(classOf[QuadTree],
      classOf[QuadTreeInfo],
      classOf[GridOperation],
      QuadTreeDigestOperations.getClass,
      QuadTreeOperations.getClass,
      Helper.getClass,
      classOf[QuadTreeInfo],
      classOf[SortedList[_]],
      classOf[Box],
      classOf[Point],
      classOf[QuadTree],
      classOf[QuadTreeDigest])
}

case class SparkKNN(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int) {

  // for testing, remove ...
  var minPartitions = 0

  def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] =
    knnJoin(rddLeft, rddRight, k).union(knnJoin(rddRight, rddLeft, k))

  def knnJoin(): RDD[(Point, Iterable[(Double, Point)])] =
    knnJoin(rddLeft, rddRight, k)

  private def knnJoin(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int): RDD[(Point, Iterable[(Double, Point)])] /*: RDD[(Point, Iterable[(Double, Point)])]*/ = {

    val (execRowCapacity, totalRowCount) = computeCapacity(rddRight, k)

    //        execRowCapacity = 57702

    //        println(">>" + execRowCapacity)

    var arrPartRangeCount = computePartitionRanges(rddRight, execRowCapacity)

    var arrPartInf = rddRight.mapPartitions(_.map(point => (point.x, point.y)))
      .repartitionAndSortWithinPartitions(new Partitioner() {

        // places in containers along the x-axis and sort by x-coor
        override def numPartitions: Int = arrPartRangeCount.length

        override def getPartition(key: Any): Int =
          key match {
            case xCoord: Double => {

              try {
                binarySearchArr(arrPartRangeCount, xCoord.toLong)
              }
              catch {
                case _: Exception =>
                  binarySearchArr(arrPartRangeCount, xCoord.toLong)
              }
            }
          }
      })
      .mapPartitionsWithIndex((pIdx, iter) => {

        val lstPartitionRangeCount = ListBuffer[PartitionInfo]()

        var right = iter.next
        var bottom = right
        var left = right
        var top = right

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
      .collect
      .sortBy(_.left._1)

    arrPartRangeCount = null

    val actualPartitionCount = AssignToPartitions(arrPartInf, execRowCapacity).getPartitionCount

    val mapUIdPartId = arrPartInf.map(partInf => (partInf.uniqueIdentifier, partInf)).toMap

    val mbrDS1 = arrPartInf.map(partInf => (partInf.left._1, partInf.bottom._2, partInf.right._1, partInf.top._2))
      .fold((Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))((mbr1, mbr2) => (math.min(mbr1._1, mbr2._1), math.min(mbr1._2, mbr2._2), math.max(mbr1._3, mbr2._3), math.max(mbr1._4, mbr2._4)))

    //    arrPartInf.foreach(pInf => println(">1>\t%s\t%s\t%s\t%s\t%d\t%d\t%d".format(pInf.left._1, pInf.bottom._2, pInf.right._1, pInf.top._2, pInf.totalPoints, pInf.assignedPart, pInf.uniqueIdentifier)))

    val rddSpIdx = rddRight
      .mapPartitions(_.map(point => (binarySearchPartInf(arrPartInf, point.x).uniqueIdentifier, point)))
      .partitionBy(new Partitioner() {
        override def numPartitions: Int = actualPartitionCount

        override def getPartition(key: Any): Int = key match {
          case uId: Int => mapUIdPartId(uId).assignedPart
        }
      })
      .mapPartitionsWithIndex((pIdx, iter) => {

        val mapSpIdx = mutable.HashMap[Int, QuadTreeInfo]()

        iter.foreach(row => {

          val partInf = mapUIdPartId(row._1) // binarySearchPartInf(arrPartInf, row._2.x)

          mapSpIdx.getOrElse(partInf.uniqueIdentifier, {

            val minX = partInf.left._1.toLong
            val minY = partInf.bottom._2.toLong
            val maxX = partInf.right._1.toLong + 1
            val maxY = partInf.top._2.toLong + 1

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
      }, preservesPartitioning = true)
      .persist(StorageLevel.MEMORY_ONLY)

//    println("=====================================")
//    rddSpIdx.foreach(qtInf => println(">2>\t%d%s%n".format(mapUIdPartId.get(qtInf.uniqueIdentifier).get.assignedPart, qtInf.toString())))
//    println("=====================================")

    // (box#, Count)
    val arrGridAndSpIdxInf = rddSpIdx
      .mapPartitionsWithIndex((pIdx, iter) => {

        val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

        iter.map(qtInf => qtInf.quadTree
          .getAllPoints
          .iterator
          .map(_.map(point => {

            //            if ("%.8f".format(point.x).equals("%.8f".format(998054.0515164168)) && "%.8f".format(point.y).equals("%.8f".format(225176.9183218691)))
            //              println(gridOp.computeBoxXY(point.x, point.y))

            (gridOp.computeBoxXY(point.x, point.y), qtInf.uniqueIdentifier)
          }))
        )
          .flatMap(_.seq)
          .flatMap(_.seq)
          .map(row => (row._1, (1L, Set(row._2))))
      })
      .reduceByKey((x, y) => (x._1 + y._1, x._2 ++ y._2))
      .collect

    //    arrGridAndSpIdxInf.foreach(row => println(">3>\t%d\t%d\t%d\t%s".format(row._1._1, row._1._2, row._2._1, row._2._2.mkString(","))))

    arrPartInf = null

    val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

    val leftBot = gridOp.computeBoxXY(mbrDS1._1, mbrDS1._2)
    val rightTop = gridOp.computeBoxXY(mbrDS1._3, mbrDS1._4)

    val halfWidth = ((rightTop._1 - leftBot._1) + 1) / 2.0
    val halfHeight = ((rightTop._2 - leftBot._2) + 1) / 2.0

    val globalIndex = new QuadTreeDigest(Box(new Point(halfWidth + leftBot._1, halfHeight + leftBot._2), new Point(halfWidth, halfHeight)))

    arrGridAndSpIdxInf.foreach(row => globalIndex.insert((row._1._1, row._1._2), row._2._1, row._2._2))

    val bvQTGlobalIndex = rddLeft.context.broadcast(globalIndex)

    var rddPoint = rddLeft
      .mapPartitions(iter => {

        val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

        iter.map(point => {

          //          if (point.userData.toString().equalsIgnoreCase("Taxi_3_B_538917"))
          //            println

          val lstUId = QuadTreeDigestOperations.getNeededSpIdxUId(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k)
            .toList

          //println(">>\t"+lstUId.size)

          //          if (lstUId.size >= 11)
          //            println(QuadTreeDigestOperations.getNeededSpIdxUId(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k))

          val tuple: Any = (point, SortedList[Point](k, allowDuplicates = false), lstUId)

          (mapUIdPartId(lstUId.head).assignedPart, tuple)
        })
      })

    //    println(rddPoint.mapPartitions(_.map(_._2.asInstanceOf[(Point, SortedList[Point], List[Int])]._3.size)).max())

    //        println("<>" + rddPoint.mapPartitions(_.map(_._2.asInstanceOf[(Point, SortSetObj, List[Int])]._3.size)).max)
    //        println(QuadTreeDigestOperations.getNeededSpIdxUId(bvQTGlobalIndex.value, gridOp.computeBoxXY(1013487.21, 134367.52), k))

    val numRounds = mapUIdPartId.values
      .map(partInf => {

        //        if (QuadTreeDigestOperations.getNeededSpIdxUId(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.bottom), k).map(mapUIdPartId(_).assignedPart).size >= 9)
        //          println(QuadTreeDigestOperations.getNeededSpIdxUId(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.bottom), k).map(mapUIdPartId(_).assignedPart))

        List(QuadTreeDigestOperations.getNeededSpIdxUId(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.left), k).map(mapUIdPartId(_).assignedPart).size,
          //                 getNeededSpIdxUId(bvGlobalIndex.value, allQTMBR, (upperRight._1 - lowerLeft._1) / 2, lowerLeft._2,k).size,
          QuadTreeDigestOperations.getNeededSpIdxUId(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.bottom), k).map(mapUIdPartId(_).assignedPart).size,
          //                 getNeededSpIdxUId(bvGlobalIndex.value, allQTMBR, upperRight._1, (upperRight._2 - lowerLeft._2) / 2,k,mapUIdPartId).size,
          QuadTreeDigestOperations.getNeededSpIdxUId(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.right), k).map(mapUIdPartId(_).assignedPart).size,
          //                 getNeededSpIdxUId(bvGlobalIndex.value, allQTMBR, (upperRight._1 - lowerLeft._1) / 2, upperRight._2,k).size,
          QuadTreeDigestOperations.getNeededSpIdxUId(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.top), k).map(mapUIdPartId(_).assignedPart).size).max
      }).max

    //    arrPartInf = null

    //        println("<>" + numRounds)

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
        .mapPartitionsWithIndex((pIdx, iter) => {

          val lstPartQT = ListBuffer[QuadTreeInfo]()

          iter.map(row => {
            row._2 match {

              case qtInf: QuadTreeInfo =>

                lstPartQT += qtInf

                null
              case _ =>

                val (point, sortSetSqDist, lstUId) = row._2.asInstanceOf[(Point, SortedList[Point], List[Int])]

                //                                if (point.userData.toString().equalsIgnoreCase("taxi_b_601998"))
                //                                    println(pIdx)

                if (lstUId.nonEmpty) {

                  // build a list of QT to check
                  val lstVisitQTInf = lstPartQT.filter(qtInf => lstUId.contains(qtInf.uniqueIdentifier))

                  //                  if (point.userData.toString().equalsIgnoreCase("Taxi_3_B_538917"))
                  //                    println

                  QuadTreeOperations.nearestNeighbor(lstVisitQTInf, point, sortSetSqDist, k)

                  val lstLeftQTInf = lstUId.filterNot(lstVisitQTInf.map(_.uniqueIdentifier).contains _)

                  (if (lstLeftQTInf.isEmpty) row._1 else mapUIdPartId(lstLeftQTInf.head).assignedPart, (point, sortSetSqDist, lstLeftQTInf))

                  //                  val ret = (if (lstLeftQTInf.isEmpty) actualPartitionCount + numRounds - 1 else mapUIdPartId(lstLeftQTInf.head).assignedPart, (point, sortSetSqDist, lstLeftQTInf))
                  //
                  //                  ret
                }
                else
                  row
            }
          })
            .filter(_ != null)
        })
    })

    rddPoint.mapPartitions(_.map(row => {

      val (point, sortSetSqDist, _) = row._2.asInstanceOf[(Point, SortedList[Point], List[Int])]

      //            val point = row._2._1 match { case pt: Point => pt }
      //            val sortSetSqDist = row._2._2

      (point, sortSetSqDist.map(nd => (nd.distance, nd.data)))
    }))
  }

  private def binarySearchArr(arrHorizDist: Array[(Double, Double)], pointX: Long): Int = {

    var topIdx = 0
    var botIdx = arrHorizDist.length - 1

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

    throw new Exception("binarySearchArr() for %,d failed in horizontal distribution %s".format(pointX, arrHorizDist.toString))
  }

  private def binarySearchPartInf(arrPartInf: Array[PartitionInfo], pointX: Double): PartitionInfo = {

    var topIdx = 0
    var botIdx = arrPartInf.length - 1

    while (botIdx >= topIdx) {

      val midIdx = (topIdx + botIdx) / 2
      val midRegion = arrPartInf(midIdx)

      if (pointX >= midRegion.left._1 && pointX <= midRegion.right._1)
        return midRegion
      else if (pointX < midRegion.left._1)
        botIdx = midIdx - 1
      else
        topIdx = midIdx + 1
    }

    throw new Exception("binarySearchPartInf() for %.8f failed in horizontal distribution %s".format(pointX, arrPartInf.toString))
  }

  //  private def getDS1Stats(iter: Iterator[Point]) = {
  //
  //    val (maxRowSize: Int, rowCount: Long, minX: Double, maxX: Double) = iter.fold(Int.MinValue, 0L, Double.MaxValue, Double.MinValue)((x, y) => {
  //
  //      val (a, b, c, d) = x.asInstanceOf[(Int, Long, Double, Double)]
  //      val point = y match {
  //        case pt: Point => pt
  //      }
  //
  //      (math.max(a, point.userData.toString.length), b + 1, math.min(c, point.x), math.max(d, point.x))
  //    })
  //
  //    (maxRowSize, rowCount, minX, maxX)
  //  }

  private def computeCapacity(rddRight: RDD[Point], k: Int) = {

    // 7% reduction in memory to account for overhead operations
    val execAvailableMemory = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory", rddRight.context.getExecutorMemoryStatus.map(_._2._1).max + "B")) // skips memory of core assigned for Hadoop daemon
    // deduct yarn overhead
    val exeOverheadMemory = math.ceil(math.max(384, 0.1 * execAvailableMemory)).toLong

    val (maxRowSize, totalRowCount) = rddRight.mapPartitionsWithIndex((pIdx, iter) =>
      Iterator(iter.map(point => (point.userData.toString.length, 1L))
        .fold(0, 0L)((param1, param2) => (math.max(param1._1, param2._1), param1._2 + param2._2)))
    )
      .fold((0, 0L))((t1, t2) => (math.max(t1._1, t2._1), t1._2 + t2._2))

    //        val gmGeomDummy = GMPoint((0 until maxRowSize).map(_ => " ").mkString(""), (0, 0))
    val userData = Array.fill[Char](maxRowSize)(' ').toString
    val pointDummy = new Point(0, 0, userData)
    val quadTreeEmptyDummy = new QuadTreeInfo(Box(new Point(pointDummy), new Point(pointDummy)))
    val sortSetDummy = SortedList[String](k, allowDuplicates = false)

    val pointCost = SizeEstimator.estimate(pointDummy)
    val sortSetCost = /* pointCost + */ SizeEstimator.estimate(sortSetDummy) + (k * pointCost)
    val quadTreeCost = SizeEstimator.estimate(quadTreeEmptyDummy)

    // exec mem cost = 1QT + 1Pt and matches
    var execRowCapacity = ((execAvailableMemory - exeOverheadMemory - quadTreeCost) / pointCost).toInt

    var numParts = math.ceil(totalRowCount.toDouble / execRowCapacity).toInt

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

        val end = row._1 + math.ceil((row._2 - row._1) * percent).toLong

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
}