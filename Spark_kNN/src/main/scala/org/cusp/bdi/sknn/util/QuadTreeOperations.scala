package org.cusp.bdi.sknn.util

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Queue
import org.cusp.bdi.util.Helper
import com.insightfullogic.quad_trees.Box
import com.insightfullogic.quad_trees.Point
import com.insightfullogic.quad_trees.QuadTree

import scala.collection.mutable

object QuadTreeOperations extends Serializable {

  def nearestNeighbor(lstQTInf: ListBuffer[QuadTreeInfo], searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int) {

    //    if (searchPoint.userData != null && searchPoint.userData.toString().equalsIgnoreCase("taxi_b_651809"))
    //      println

    var searchRegion: Box = null

    if (sortSetSqDist.isEmpty()) {

      val sPtBestQT = getBestQuadrant(lstQTInf.head, (searchPoint.x, searchPoint.y), k)

      //            val dim = math.max(sPtBestQT.boundary.halfDimension.x + math.abs(searchPoint.x - sPtBestQT.boundary.center.x), sPtBestQT.boundary.halfDimension.y + math.abs(searchPoint.y - sPtBestQT.boundary.center.y))

      val dim = math.ceil(math.sqrt(getFurthestCorner(searchPoint, sPtBestQT)._1))

      searchRegion = Box(searchPoint, new Point(dim, dim))
    }
    else {

      val maxDist = math.sqrt(sortSetSqDist.last().distance)

      searchRegion = Box(searchPoint, new Point(maxDist, maxDist))
    }

    lstQTInf.foreach(qtInf => pointsWithinRegion(qtInf, searchRegion, sortSetSqDist))
  }

  private def getFurthestCorner(searchPoint: Point, sPtBestQT: QuadTree) = {

    val qtdLeft = sPtBestQT.boundary.left
    val qtdRight = sPtBestQT.boundary.right
    val qtdBottom = sPtBestQT.boundary.bottom
    val qtdTop = sPtBestQT.boundary.top

    Array((qtdLeft, qtdBottom), (qtdRight, qtdBottom), (qtdRight, qtdTop), (qtdLeft, qtdTop))
      .map(xy => (Helper.squaredDist(xy._1, xy._2, searchPoint.x, searchPoint.y), xy))
      .maxBy(_._1)
  }

  private def contains(qTree: QuadTree, xy: (Double, Double), k: Int) =
    qTree != null && qTree.getTotalPoints > k && qTree.boundary.contains(xy._1, xy._2)

  private def intersects(qTree: QuadTree, searchRegion: Box) =
    qTree != null && searchRegion.intersects(qTree.boundary)

  private def getBestQuadrant(qtInf: QuadTreeInfo, xy: (Double, Double), k: Int) = {

    // find leaf containing point
    var done = false
    var qTree = qtInf.quadTree //lstChainQT.head

    while (!done)
      if (contains(qTree.topLeft, xy, k))
        qTree = qTree.topLeft
      else if (contains(qTree.topRight, xy, k))
        qTree = qTree.topRight
      else if (contains(qTree.bottomLeft, xy, k))
        qTree = qTree.bottomLeft
      else if (contains(qTree.bottomRight, xy, k))
        qTree = qTree.bottomRight
      else
        done = true

    qTree
  }

  private def pointsWithinRegion(qtInf: QuadTreeInfo, searchRegion: Box, sortSetSqDist: SortedList[Point]) {

    if (searchRegion.intersects(qtInf.quadTree.boundary)) {

      val queueQT = mutable.Queue(qtInf.quadTree)

      var prevMaxSqrDist = if (sortSetSqDist.isEmpty()) -1 else sortSetSqDist.last().distance

      while (queueQT.nonEmpty) {

        val qTree = queueQT.dequeue()

        qTree.getPoints
          .foreach(qtPoint => {

            if (searchRegion.contains(qtPoint)) {

              //              if (qtPoint.userData.toString().equalsIgnoreCase("Taxi_A_27652"))
              //                println()

              val sqDist = Helper.squaredDist(searchRegion.center.x, searchRegion.center.y, qtPoint.x, qtPoint.y)

              sortSetSqDist.add(sqDist, qtPoint)

              if (sortSetSqDist.isFull && prevMaxSqrDist != sortSetSqDist.last().distance) {

                prevMaxSqrDist = sortSetSqDist.last().distance

                val newDim = math.sqrt(prevMaxSqrDist)

                searchRegion.halfDimension.x = newDim
                searchRegion.halfDimension.y = newDim
              }
            }
          })

        if (intersects(qTree.topLeft, searchRegion))
          queueQT += qTree.topLeft
        if (intersects(qTree.topRight, searchRegion))
          queueQT += qTree.topRight
        if (intersects(qTree.bottomLeft, searchRegion))
          queueQT += qTree.bottomLeft
        if (intersects(qTree.bottomRight, searchRegion))
          queueQT += qTree.bottomRight
      }
    }
  }

//  private def buildSearchRegion(qtInf: QuadTreeInfo, searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int) = {
//
//    val searchDim = if (sortSetSqDist.isEmpty()) {
//
//      val sPtBestQT = getBestQuadrant(qtInf, (searchPoint.x, searchPoint.y), k)
//
//      (sPtBestQT.boundary.halfDimension.x + math.abs(searchPoint.x - sPtBestQT.boundary.center.x), sPtBestQT.boundary.halfDimension.y + math.abs(searchPoint.y - sPtBestQT.boundary.center.y))
//    }
//    else {
//
//      val maxDist = math.sqrt(sortSetSqDist.last.distance)
//
//      (maxDist, maxDist)
//    }
//
//    new Box(searchPoint, new Point(searchDim._1, searchDim._2))
//  }
}