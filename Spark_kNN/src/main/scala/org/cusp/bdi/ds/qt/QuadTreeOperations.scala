package org.cusp.bdi.ds.qt

import org.cusp.bdi.ds.{Box, Point}
import org.cusp.bdi.sknn.GlobalIndexPointData
import org.cusp.bdi.sknn.util.{Node, QuadTreeInfo, SortedList}
import org.cusp.bdi.util.Helper

import scala.collection.mutable.ListBuffer

object QuadTreeOperations extends Serializable {

  def nearestNeighbor(lstQTInf: ListBuffer[QuadTreeInfo], searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int) {

    //    if (searchPoint.userData != null && searchPoint.userData.toString().equalsIgnoreCase("taxi_b_651809"))
    //      println

    lstQTInf.foreach(qtInf => {

      //      if (searchPoint.userData.toString().equalsIgnoreCase("bread_2_a_598733"))
      //        println()

      var searchRegion: Box = null

      var sPtBestQT: QuadTree = null

      var dim = 0.0

      if (sortSetSqDist.isFull)
        dim = math.sqrt(sortSetSqDist.last().distance)
      else {

        sPtBestQT = getBestQuadrant(qtInf.quadTree, searchPoint, k)

        //      val dim = math.ceil(math.sqrt(getFurthestCorner(searchPoint, sPtBestQT)._1))

        dim = math.max(math.max(math.abs(searchPoint.x - sPtBestQT.boundary.left), math.abs(searchPoint.x - sPtBestQT.boundary.right)),
          math.max(math.abs(searchPoint.y - sPtBestQT.boundary.bottom), math.abs(searchPoint.y - sPtBestQT.boundary.top)))
      }

      searchRegion = Box(searchPoint, new Point(dim, dim))

      if (sPtBestQT != null)
        pointsWithinRegion(sPtBestQT, null, searchRegion, sortSetSqDist)

      if (qtInf.quadTree != sPtBestQT)
        pointsWithinRegion(qtInf.quadTree, sPtBestQT, searchRegion, sortSetSqDist)
    })
  }

  private def pointsWithinRegion(quadTree: QuadTree, skipQuadTree: QuadTree, searchRegion: Box, sortSetSqDist: SortedList[Point]) {

    //    if (searchRegion.intersects(quadTree.boundary)) {

    val lstQT = ListBuffer(quadTree)

    var prevMaxSqrDist = if (sortSetSqDist.isEmpty()) -1 else sortSetSqDist.last().distance

    lstQT.foreach(qTree =>
      if (qTree != skipQuadTree) {

        qTree.getLstPoint
          .filter(searchRegion.contains)
          .foreach(qtPoint => {

            //            if (qtPoint.userData.toString().equalsIgnoreCase("Bread_2_B_27676"))
            //              print("")

            val sqDist = Helper.squaredDist(searchRegion.center.x, searchRegion.center.y, qtPoint.x, qtPoint.y)

            sortSetSqDist.add(sqDist, qtPoint)

            if (sortSetSqDist.isFull && prevMaxSqrDist != sortSetSqDist.last().distance) {

              prevMaxSqrDist = sortSetSqDist.last().distance

              searchRegion.halfDimension.x = math.sqrt(prevMaxSqrDist)
              searchRegion.halfDimension.y = searchRegion.halfDimension.x
            }
          })

        if (intersects(qTree.topLeft, searchRegion))
          lstQT += qTree.topLeft
        if (intersects(qTree.topRight, searchRegion))
          lstQT += qTree.topRight
        if (intersects(qTree.bottomLeft, searchRegion))
          lstQT += qTree.bottomLeft
        if (intersects(qTree.bottomRight, searchRegion))
          lstQT += qTree.bottomRight
      }
    )
    //    }
  }

  private def intersects(quadTree: QuadTree, searchRegion: Box) =
    quadTree != null && searchRegion.intersects(quadTree.boundary)

  private def getBestQuadrant(quadTree: QuadTree, searchPoint: Point, k: Int) = {

    // find leaf containing point
    var done = false
    var qTree = quadTree

    def testQuad(qtQuad: QuadTree) =
      qtQuad != null && qtQuad.getTotalPoints >= k && qtQuad.boundary.contains(searchPoint.x, searchPoint.y)

    while (!done)
      if (testQuad(qTree.topLeft))
        qTree = qTree.topLeft
      else if (testQuad(qTree.topRight))
        qTree = qTree.topRight
      else if (testQuad(qTree.bottomLeft))
        qTree = qTree.bottomLeft
      else if (testQuad(qTree.bottomRight))
        qTree = qTree.bottomRight
      else
        done = true

    qTree
  }

  def spatialIdxRangeLookup(quadree: QuadTree, searchPointXY: (Long, Long), k: Int, expandBy: Double): Set[Int] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    val searchPoint = new Point(searchPointXY._1, searchPointXY._2)

    val sPtBestQT = getBestQuadrant(quadree, searchPoint, k)

    val dim = expandBy + math.max(math.max(math.abs(searchPoint.x - sPtBestQT.boundary.left), math.abs(searchPoint.x - sPtBestQT.boundary.right)),
      math.max(math.abs(searchPoint.y - sPtBestQT.boundary.bottom), math.abs(searchPoint.y - sPtBestQT.boundary.top)))

    val searchRegion = Box(searchPoint, new Point(dim, dim))

    val sortList = spatialIdxRangeLookupHelper(sPtBestQT, quadree, searchRegion, k, expandBy)

    sortList
      .map(f = _.data.userData match {
        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.setUId
      })
      .flatMap(_.seq)
      .toSet
  }

  private def spatialIdxRangeLookupHelper(quadTreeStart: QuadTree, quadTree: QuadTree, searchRegion: Box, k: Int, expandBy: Double) = {

    //    var totalCount = 0

    val sortList = SortedList[Point](Int.MaxValue, true)
    var prevLastElem = sortList.head
    var currSqDim = math.pow(searchRegion.halfDimension.x, 2)
    var weight = 0L

    var lstQT = ListBuffer(quadTreeStart)

    def getNumPoints(point: Point): Long = point.userData match {
      case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.numPoints
    }

    def process(startRound: Boolean) {

      lstQT.foreach(qTree =>
        if (startRound || qTree != quadTreeStart) {

          qTree.getLstPoint
            .filter(searchRegion.contains)
            .foreach(qtPoint => {

              //              if (qtPoint.x.toString().startsWith("26157") && qtPoint.y.toString().startsWith("4965"))
              //                print("")

              val sqDist = Helper.squaredDist(searchRegion.center.x, searchRegion.center.y, qtPoint.x, qtPoint.y)

              if (prevLastElem == null || sqDist <= currSqDim) {

                sortList.add(sqDist, qtPoint)

                weight += getNumPoints(qtPoint)

                if ((qtPoint != sortList.last.data || prevLastElem == null) && (weight - getNumPoints(sortList.last.data)) >= k) {

                  var elem = sortList.head
                  weight = getNumPoints(elem.data)

                  while (weight < k) {

                    elem = elem.next

                    weight += getNumPoints(elem.data)
                  }

                  if (elem != prevLastElem) {

                    prevLastElem = elem

                    searchRegion.halfDimension.x = math.sqrt(prevLastElem.distance).toLong + 1 + expandBy
                    searchRegion.halfDimension.y = searchRegion.halfDimension.x

                    currSqDim = math.pow(searchRegion.halfDimension.x, 2)

                    // keep points with xCoord < than the center's
                  }

                  if (sortList.last.distance > currSqDim) {

                    while (elem.next != null && elem.next.distance <= currSqDim) {

                      elem = elem.next

                      weight += getNumPoints(elem.data)
                    }

                    sortList.discardAfter(elem)
                  }
                }
              }
            })

          if (intersects(qTree.topLeft, searchRegion))
            lstQT += qTree.topLeft
          if (intersects(qTree.topRight, searchRegion))
            lstQT += qTree.topRight
          if (intersects(qTree.bottomLeft, searchRegion))
            lstQT += qTree.bottomLeft
          if (intersects(qTree.bottomRight, searchRegion))
            lstQT += qTree.bottomRight
        })
    }

    process(true)

    if (quadTreeStart != quadTree) {

      lstQT = ListBuffer(quadTree)
      process(false)
    }

    sortList
  }

  //  private def getFurthestCorner(searchPoint: Point, sPtBestQT: QuadTree) = {
  //
  //    val qtdLeft = sPtBestQT.boundary.left
  //    val qtdRight = sPtBestQT.boundary.right
  //    val qtdBottom = sPtBestQT.boundary.bottom
  //    val qtdTop = sPtBestQT.boundary.top
  //
  //    Array((qtdLeft, qtdBottom), (qtdRight, qtdBottom), (qtdRight, qtdTop), (qtdLeft, qtdTop))
  //      .map(xy => (Helper.squaredDist(xy._1, xy._2, searchPoint.x, searchPoint.y), xy))
  //      .maxBy(_._1)
  //  }
}