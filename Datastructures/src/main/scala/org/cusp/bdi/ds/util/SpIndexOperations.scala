package org.cusp.bdi.ds.util

import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.{Box, Point}
import org.cusp.bdi.util.{Node, SortedList}

import scala.collection.mutable.ListBuffer

object SpIndexOperations extends Serializable {

  def nearestNeighbor(lstQTInf: ListBuffer[SpIndexInfo], searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int) {

    //    if (searchPoint.userData != null && searchPoint.userData.toString().equalsIgnoreCase("taxi_b_651809"))
    //      println

    lstQTInf.foreach(qtInf => {

      //      if (searchPoint.userData.toString().equalsIgnoreCase("bread_2_a_598733"))
      //        rln()

      var searchRegion: Box = null

      var sPtBestQT: QuadTree = null

      var dim = 0.0

      if (sortSetSqDist.isFull)
        dim = math.sqrt(sortSetSqDist.last().distance)
      else {

        val qt = qtInf.dataStruct match {
          case qt: QuadTree => qt
        }

        sPtBestQT = getBestQuadrant(qt, searchPoint, k)

        //      val dim = math.ceil(math.sqrt(getFurthestCorner(searchPoint, sPtBestQT)._1))

        dim = math.max(math.max(math.abs(searchPoint.x - sPtBestQT.boundary.left), math.abs(searchPoint.x - sPtBestQT.boundary.right)),
          math.max(math.abs(searchPoint.y - sPtBestQT.boundary.bottom), math.abs(searchPoint.y - sPtBestQT.boundary.top)))
      }

      searchRegion = Box(searchPoint, new Point(dim, dim))

      if (sPtBestQT != null)
        pointsWithinRegion(sPtBestQT, null, searchRegion, sortSetSqDist)

      if (qtInf.dataStruct != sPtBestQT) {

        val qt = qtInf.dataStruct match {
          case qt: QuadTree => qt
        }

        pointsWithinRegion(qt, sPtBestQT, searchRegion, sortSetSqDist)
      }
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

            val sqDist = squaredDist(searchRegion.center.x, searchRegion.center.y, qtPoint.x, qtPoint.y)

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

  def spatialIdxRangeLookup(quadree: QuadTree, searchPointGridXY: (Long, Long), k: Int, errorRange: Double): Set[Int] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    //    val xy = gridOperation.computeBoxXY(searchPoint.x, searchPoint.y)

    val centerPoint = new Point(searchPointGridXY._1.toDouble, searchPointGridXY._2.toDouble)

    val sPtBestQT = getBestQuadrant(quadree, centerPoint, k)

    val dim = math.max(math.max(math.abs(centerPoint.x - sPtBestQT.boundary.left), math.abs(centerPoint.x - sPtBestQT.boundary.right)),
      math.max(math.abs(centerPoint.y - sPtBestQT.boundary.bottom), math.abs(centerPoint.y - sPtBestQT.boundary.top)))

    val searchRegion = Box(centerPoint, new Point(dim, dim))

    val sortList = spatialIdxRangeLookupHelper(sPtBestQT, quadree, searchRegion, k, errorRange)

    sortList
      .map(f = _.data.userData.asInstanceOf[Set[Int]])
      .flatMap(_.seq)
      .toSet
  }

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

  private def spatialIdxRangeLookupHelper(quadTreeStart: QuadTree, quadTree: QuadTree, searchRegion: Box, k: Int, errorRange: Double) = {

    //    var totalCount = 0

    val sortList = SortedList[Point](Int.MaxValue, allowDuplicates = true)
    var prevLastElem: Node[Point] = null
    var currSqDim = math.pow(searchRegion.halfDimension.x, 2)

    var lstQT = ListBuffer(quadTreeStart)

    def process(startRound: Boolean) {

      lstQT.foreach(qTree =>
        if (startRound || qTree != quadTreeStart) {

          qTree.getLstPoint
            .filter(point => startRound || searchRegion.contains(point))
            .foreach(qtPoint => {

              //              if (qtPoint.x.toString().startsWith("167404") && qtPoint.y.toString().startsWith("32479"))
              //                println("")

              val sqDist = squaredDist(searchRegion.center.x, searchRegion.center.y, qtPoint.x, qtPoint.y)

              if (prevLastElem == null || sqDist < currSqDim) {

                sortList.add(sqDist, qtPoint)

                if (sortList.size >= k) {

                  var elem = sortList.get(k - 1)

                  if (elem != prevLastElem) {

                    prevLastElem = elem

                    searchRegion.halfDimension.x = math.sqrt(prevLastElem.distance) /*.toLong + 1*/ + errorRange
                    searchRegion.halfDimension.y = searchRegion.halfDimension.x

                    currSqDim = math.pow(searchRegion.halfDimension.x, 2)

                    // keep points with xCoord < than the center's
                    var count = k

                    while (elem.next != null && elem.next.distance < currSqDim) {

                      elem = elem.next
                      count += 1
                    }

                    sortList.discardAfter(elem, count)
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

  private def intersects(quadTree: QuadTree, searchRegion: Box) =
    quadTree != null && searchRegion.intersects(quadTree.boundary)

  private def squaredDist(x1: Double, y1: Double, x2: Double, y2: Double) =
    math.pow(x1 - x2, 2) + math.pow(y1 - y2, 2)
}