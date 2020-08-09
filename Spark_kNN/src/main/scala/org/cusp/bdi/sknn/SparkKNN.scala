package org.cusp.bdi.sknn

import com.insightfullogic.quad_trees.{Box, Point, QuadTree}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.sknn.util._
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

case class PartitionInfo(left: (Double, Double), bottom: (Double, Double), right: (Double, Double), top: (Double, Double), uniqueIdentifier: Int, totalPoints: Long) {

  var assignedPart: Int = -1

  override def toString: String =
    "%d\t%d\t%d".format(assignedPart, uniqueIdentifier, totalPoints)
}

object SparkKNN {

  def getSparkKNNClasses: Array[Class[_]] =
    Array(classOf[QuadTree],
      classOf[QuadTreeInfo],
      classOf[GridOperation],
      QuadTreeOperations.getClass,
      Helper.getClass,
      classOf[QuadTreeInfo],
      classOf[SortedList[_]],
      classOf[Box],
      classOf[Point],
      classOf[QuadTree])
}

case class SparkKNN(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int) {

  // for testing, remove ...
  var minPartitions = 0

  def allKnnJoin(): RDD[(Point, Iterable[(Double, Point)])] =
    knnJoinExecute(rddLeft, rddRight, k).union(knnJoinExecute(rddRight, rddLeft, k))

  private def knnJoinExecute(rddLeft: RDD[Point], rddRight: RDD[Point], k: Int): RDD[(Point, Iterable[(Double, Point)])] /*: RDD[(Point, Iterable[(Double, Point)])]*/ = {

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

        //        if ("%.8f".format(left._1).startsWith("%.8f".format(218892.35631916218)) && "%.8f".format(left._2).startsWith("%.8f".format(1949299.3119498254)))
        //          println(pIdx)

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
      } /*, preservesPartitioning = true*/)
      .persist(StorageLevel.MEMORY_ONLY)

    //    println(">2>=====================================")
    //    rddSpIdx.foreach(qtInf => println(">2>\t%d%s%n".format(mapUIdPartId(qtInf.uniqueIdentifier).assignedPart, qtInf.toString())))
    //    println(">2>=====================================")

    // (box#, Count)
    val arrGridAndSpIdxInf = rddSpIdx
      .mapPartitions(iter => {

        val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

        iter.map(qtInf => qtInf.quadTree
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

    arrPartInf = null

    val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

    val leftBot = gridOp.computeBoxXY(mbrDS1._1, mbrDS1._2)
    val rightTop = gridOp.computeBoxXY(mbrDS1._3, mbrDS1._4)

    val halfWidth = ((rightTop._1 - leftBot._1) + 1) / 2.0
    val halfHeight = ((rightTop._2 - leftBot._2) + 1) / 2.0

    val globalIndex = new QuadTree(Box(new Point(halfWidth + leftBot._1, halfHeight + leftBot._2), new Point(halfWidth, halfHeight)))

    arrGridAndSpIdxInf.foreach(row => globalIndex.insert(new Point(row._1._1, row._1._2, row._2)))

    val bvQTGlobalIndex = rddLeft.context.broadcast(globalIndex)

    var rddPoint = rddLeft
      .mapPartitions(iter => {

        val gridOp = new GridOperation(mbrDS1, totalRowCount, k)

        iter.map(point => {

          //          if (point.userData.toString().equalsIgnoreCase("bread_2_a_598733"))
          //            println

          val lstUId = QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k)
            .toList

          //println(">>\t"+lstUId.size)

          //          if (lstUId.size >= 11)
          //            println(QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(point.x, point.y), k))

          val tuple: Any = (point, SortedList[Point](k, allowDuplicates = false), lstUId)

          (mapUIdPartId(lstUId.head).assignedPart, tuple)
        })
      })

    //    println(rddPoint.map(_._2.asInstanceOf[(Point, SortedList[Point], List[Int])]._3.size).max)

    val numRounds = mapUIdPartId.values
      .map(partInf => {

        //        if (QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.left), k).map(mapUIdPartId(_).assignedPart).size >= 9)
        //          println(QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.left), k).map(mapUIdPartId(_).assignedPart))

        List(QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.left), k).map(mapUIdPartId(_).assignedPart).size,
          QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.bottom), k).map(mapUIdPartId(_).assignedPart).size,
          QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.right), k).map(mapUIdPartId(_).assignedPart).size,
          QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.top), k).map(mapUIdPartId(_).assignedPart).size,
          QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.left._1, partInf.bottom._2), k).map(mapUIdPartId(_).assignedPart).size,
          QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.right._1, partInf.bottom._2), k).map(mapUIdPartId(_).assignedPart).size,
          QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.right._1, partInf.top._2), k).map(mapUIdPartId(_).assignedPart).size,
          QuadTreeOperations.spatialIdxRangeLookup(bvQTGlobalIndex.value, gridOp.computeBoxXY(partInf.left._1, partInf.top._2), k).map(mapUIdPartId(_).assignedPart).size).max
      }).max

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
              case partId: Int => if (partId < 0) -partId else partId
            }
        })
        .mapPartitions(iter => {

          val lstPartQT = ListBuffer[QuadTreeInfo]()

          iter.map(row => {

            if (row._1 < 0)
              row
            else
              row._2 match {

                case qtInf: QuadTreeInfo =>

                  lstPartQT += qtInf

                  null
                case _ =>

                  val (point, sortSetSqDist, lstUId) = row._2.asInstanceOf[(Point, SortedList[Point], List[Int])]

                  if (lstUId == null)
                    row
                  else {

                    //                if ((pIdx == 16 || pIdx == 8) && point.userData.toString().equalsIgnoreCase("taxi_2_b_731083"))
                    //                  println(pIdx)

                    // build a list of QT to check
                    val lstVisitQTInf = lstPartQT.filter(partQT => lstUId.contains(partQT.uniqueIdentifier))
                    //                    .sortBy(!_.quadTree.boundary.contains(point))

                    //                  if (point.userData.toString().equalsIgnoreCase("bread_2_a_598733"))
                    //                    println(pIdx)

                    QuadTreeOperations.nearestNeighbor(lstVisitQTInf, point, sortSetSqDist, k)

                    // randomize order to lessen query skews
                    val lstLeftQTInf = Random.shuffle(lstUId.filterNot(lstVisitQTInf.map(_.uniqueIdentifier).contains _))

                    // send done rows to a random partition to lessen query skews.
                    (if (lstLeftQTInf.isEmpty) Random.nextInt(actualPartitionCount) else mapUIdPartId(lstLeftQTInf.head).assignedPart, (point, sortSetSqDist, if (lstLeftQTInf.isEmpty) null else lstLeftQTInf))
                  }
              }
          })
            .filter(_ != null)
        })
    })

    rddPoint.mapPartitions(_.map(row => {

      val (point, sortSetSqDist, _) = row._2.asInstanceOf[(Point, SortedList[Point], _)]

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

    throw new Exception("binarySearchArr() for %,d failed in horizontal distribution %s".format(pointX, arrHorizDist.mkString("Array(", ", ", ")")))
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

    throw new Exception("binarySearchPartInf() for %.8f failed in horizontal distribution %s".format(pointX, arrPartInf.mkString("Array(", ", ", ")")))
  }

  private def computeCapacity(rddRight: RDD[Point], k: Int): (Int, Long) = {

    // 7% reduction in memory to account for overhead operations
    val execAvailableMemory = Helper.toByte(rddRight.context.getConf.get("spark.executor.memory", rddRight.context.getExecutorMemoryStatus.map(_._2._1).max + "B")) // skips memory of core assigned for Hadoop daemon
    // deduct yarn overhead
    val exeOverheadMemory = math.ceil(math.max(384, 0.1 * execAvailableMemory)).toLong

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

  def knnJoin() =
    knnJoinExecute(rddLeft, rddRight, k)
}