package org.cusp.bdi.sknn.util

import scala.collection.mutable.Queue
import org.cusp.bdi.util.Helper
import com.insightfullogic.quad_trees.Box
import com.insightfullogic.quad_trees.Point
import com.insightfullogic.quad_trees.QuadTreeDigest

import scala.collection.mutable

object QuadTreeDigestOperations {

  //  private val dimExtend = 3 * math.sqrt(2) // accounts for the further points effected by precision loss when converting to gird

  def getNeededSpIdxUId(quadTreeDigest: QuadTreeDigest, searchPointXY: (Long, Long), k: Int /*, gridBoxMaxDim: Double*/): Set[Int] = {

    //        if (searchPointXY._1.toString().startsWith("15221") && searchPointXY._2.toString().startsWith("3205"))
    //            //            //            //            //            //            //        if (searchPointXY._1.toString().startsWith("100648") && searchPointXY._2.toString().startsWith("114152"))
    //            println

    val searchPoint = new Point(searchPointXY._1, searchPointXY._2)

    val sPtBestQTD = getBestQuadrant(quadTreeDigest, searchPoint, k)

    //        val dim = math.ceil(dimExtend + math.max(sPtBestQTD.boundary.halfDimension.x + math.abs(searchPoint.x - sPtBestQTD.boundary.center.x), sPtBestQTD.boundary.halfDimension.y + math.abs(searchPoint.y - sPtBestQTD.boundary.center.y)))
    //        val searchRegion = new Box(searchPoint, new Point(dim, dim))

    //        val searchRegion = new Box(searchPoint, new Point((sPtBestQTD.boundary.halfDimension.x + math.abs(searchPoint.x - sPtBestQTD.boundary.center.x), sPtBestQTD.boundary.halfDimension.y + math.abs(searchPoint.y - sPtBestQTD.boundary.center.y)) /* + gridBoxH */ ))

    val dim = math.ceil(/*dimExtend +*/ math.sqrt(getFurthestCorner(searchPoint, sPtBestQTD)._1))

    val searchRegion = Box(searchPoint, new Point(dim, dim))

    // val sortSetSqDist = SortSetObj(Int.MaxValue)

    var sortList = SortedList[Point](Int.MaxValue, allowDuplicates = true)

    //         sortList = pointsWithinRegion(quadTreeDigest, searchRegion, k, sortList, null, gridBoxW, gridBoxH)

    sortList = pointsWithinRegion(sPtBestQTD, searchRegion, k, sortList, null)

    if (quadTreeDigest != sPtBestQTD)
      sortList = pointsWithinRegion(quadTreeDigest, searchRegion, k, sortList, sPtBestQTD)

    //        println(sortList.size)
    //        val distCap = sortSetSqDist.last.distance + gridBoxSqDiag

    sortList
      .map(_.data.userData.asInstanceOf[(Long, Set[Int])]._2)
      .flatMap(_.seq)
      .toSet
  }

  private def getBestQuadrant(qtDigest: QuadTreeDigest, searchPoint: Point, k: Int) = {

    // find leaf containing point
    var done = false
    var qtd = qtDigest

    while (!done)
      if (contains(qtd.topLeft, searchPoint, k))
        qtd = qtd.topLeft
      else if (contains(qtd.topRight, searchPoint, k))
        qtd = qtd.topRight
      else if (contains(qtd.bottomLeft, searchPoint, k))
        qtd = qtd.bottomLeft
      else if (contains(qtd.bottomRight, searchPoint, k))
        qtd = qtd.bottomRight
      else
        done = true

    qtd
  }

  private def contains(qtd: QuadTreeDigest, searchPoint: Point, k: Int) =
    qtd != null && qtd.getTotalPointWeight > k && qtd.boundary.contains(searchPoint.x, searchPoint.y)

  private def pointsWithinRegion(quadTreeDigest: QuadTreeDigest, searchRegion: Box, k: Int, sortList: SortedList[Point], skipQTD: QuadTreeDigest /*, gridBoxMaxDim: Double*/) = {

    var totalWeight = if (sortList.isEmpty()) 0 else sortList.map(_.data match { case pt: Point => pt.userData.asInstanceOf[(Long, Set[Int])]._1 }).sum

    val sortListTemp = sortList
    var prevLastElem = if (sortList.isEmpty()) null else sortList.last()
    var currSqDim = if (sortList.isFull) prevLastElem.distance else math.pow(searchRegion.halfDimension.x, 2)

    def shrinkSearchRegion() {

      if (totalWeight - sortListTemp.last().data.userData.asInstanceOf[(Long, Set[Int])]._1 >= k) {

        val iter = sortListTemp.iterator()
        var elem = iter.next
        var weightSoFar = elem.data.userData.asInstanceOf[(Long, Set[Int])]._1

        var idx = 0
        while (iter.hasNext && (weightSoFar < k || elem.distance == 0)) {

          idx += 1
          elem = iter.next
          weightSoFar += elem.data.userData.asInstanceOf[(Long, Set[Int])]._1
        }

        if (elem != prevLastElem) {

          prevLastElem = elem

          val newDim = math.sqrt(elem.distance) /*+ dimExtend*/
          searchRegion.halfDimension.x = newDim
          searchRegion.halfDimension.y = newDim

          currSqDim = math.pow(newDim, 2)

          if (idx < sortListTemp.size - 1 && sortListTemp.last().distance > currSqDim) {

            var done = false
            idx += 1

            do {

              elem = iter.next

              if (elem.distance <= currSqDim) {

                weightSoFar += elem.data.userData.asInstanceOf[(Long, Set[Int])]._1
                idx += 1
              }
              else
                done = true
            } while (!done && idx < sortListTemp.size)

            if (idx < sortListTemp.size) {

              totalWeight = weightSoFar

              sortListTemp.discardAfter(idx)
            }
          }
        }
      }
    }

    val queueQT = mutable.Queue(quadTreeDigest)

    while (queueQT.nonEmpty) {

      val qtd = queueQT.dequeue()

      //            if (qtd.boundary.center.x.toString().startsWith("15206") && qtd.boundary.center.y.toString().startsWith("3362"))
      //                println

      qtd.getLstPoint
        .foreach(qtPoint => {

          //          if (qtd.boundary.contains(new Point(6160, 1389)) && !qtd.getLstPoint.filter(pt => pt.x.toInt == 6160 && pt.y.toInt == 1389).isEmpty) {
          //
          //            println(searchRegion.left)
          //            println(searchRegion.right)
          //            println(searchRegion.bottom)
          //            println(searchRegion.top)
          //
          //            println(qtPoint.x < searchRegion.left)
          //            println(qtPoint.x > searchRegion.right)
          //            println(qtPoint.y < searchRegion.bottom)
          //            println(qtPoint.y > searchRegion.top)
          //          }

          if (searchRegion.contains(qtPoint)) {

            val sqDist = Helper.squaredDist(searchRegion.center.x, searchRegion.center.y, qtPoint.x, qtPoint.y)

            if (prevLastElem == null || sqDist <= currSqDim) {

              sortListTemp.add(sqDist, qtPoint) // PointWithUniquId(qtd.getSetPart, qtPoint))

              totalWeight += qtPoint.userData.asInstanceOf[(Long, Set[Int])]._1

              if (totalWeight > k)
                shrinkSearchRegion()
            }
          }
        })

      //      else if (qtd.topLeft != null && qtd.topLeft.boundary.contains())
      //        print()
      //      else if (qtd.topRight != null && qtd.topRight.boundary.contains(new Point(6160, 1389)))
      //        print()
      //      else if (qtd.bottomLeft != null && qtd.bottomLeft.boundary.contains(new Point(6160, 1389)))
      //        print()
      //      else if (qtd.bottomRight != null && qtd.bottomRight.boundary.contains(new Point(6160, 1389)))
      //        print()

      if (intersects(qtd.topLeft, searchRegion, skipQTD))
        queueQT += qtd.topLeft
      if (intersects(qtd.topRight, searchRegion, skipQTD))
        queueQT += qtd.topRight
      if (intersects(qtd.bottomLeft, searchRegion, skipQTD))
        queueQT += qtd.bottomLeft
      if (intersects(qtd.bottomRight, searchRegion, skipQTD))
        queueQT += qtd.bottomRight
    }

    sortListTemp
  }

  private def intersects(qtd: QuadTreeDigest, searchRegion: Box, skipQTD: QuadTreeDigest) =
    qtd != null && qtd != skipQTD && searchRegion.intersects(qtd.boundary)

  private def getFurthestCorner(searchPoint: Point, sPtBestQTD: QuadTreeDigest) = {

    val qtdLeft = sPtBestQTD.boundary.left
    val qtdRight = sPtBestQTD.boundary.right
    val qtdBottom = sPtBestQTD.boundary.bottom
    val qtdTop = sPtBestQTD.boundary.top

    Array((qtdLeft, qtdBottom), (qtdRight, qtdBottom), (qtdRight, qtdTop), (qtdLeft, qtdTop))
      .map(xy => (Helper.squaredDist(xy._1, xy._2, searchPoint.x, searchPoint.y), xy))
      .maxBy(_._1)
  }
}