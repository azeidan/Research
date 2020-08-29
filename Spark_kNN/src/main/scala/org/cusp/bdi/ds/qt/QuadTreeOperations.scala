package org.cusp.bdi.ds.qt

import org.cusp.bdi.ds.{Box, Point}
import org.cusp.bdi.sknn.GlobalIndexPointData
import org.cusp.bdi.util.{Helper, SortedList}

import scala.collection.mutable.ListBuffer

object QuadTreeOperations extends Serializable {

  private val expandBy = math.sqrt(8) // 2 * math.sqrt(2)

  def nearestNeighbor(quadTree: QuadTree, searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int) {

    //    if (searchPoint.userData.toString().equalsIgnoreCase("yellow_3_a_772558"))
    //      println

    var searchRegion: Box = null

    var sPtBestQT: QuadTree = null

    var dim = 0.0

    if (sortSetSqDist.isFull)
      dim = math.sqrt(sortSetSqDist.last().distance)
    else {

      sPtBestQT = getBestQuadrant(quadTree, searchPoint, k)

      //      val dim = math.ceil(math.sqrt(getFurthestCorner(searchPoint, sPtBestQT)._1))

      dim = math.max(math.max(math.abs(searchPoint.x - sPtBestQT.boundary.left), math.abs(searchPoint.x - sPtBestQT.boundary.right)),
        math.max(math.abs(searchPoint.y - sPtBestQT.boundary.bottom), math.abs(searchPoint.y - sPtBestQT.boundary.top)))
    }

    searchRegion = Box(searchPoint, new Point(dim, dim))

    pointsWithinRegion(sPtBestQT, quadTree, searchRegion, sortSetSqDist)

    //      if (sPtBestQT != null)
    //        pointsWithinRegion(sPtBestQT, null, searchRegion, sortSetSqDist)
    //
    //      if (qtInf.quadTree != sPtBestQT)
    //        pointsWithinRegion(qtInf.quadTree, sPtBestQT, searchRegion, sortSetSqDist)
  }

  private def pointsWithinRegion(quadTreeFirst: QuadTree, quadTreeSecond: QuadTree, searchRegion: Box, sortSetSqDist: SortedList[Point]) {

    //    if (searchRegion.intersects(quadTree.boundary)) {

    var lstQT = ListBuffer(quadTreeFirst)

    var prevMaxSqrDist = if (sortSetSqDist.isEmpty()) -1 else sortSetSqDist.last().distance

    def process(startRound: Boolean) {
      lstQT.foreach(qTree =>
        if (startRound || qTree != quadTreeFirst) {

          qTree.getLstPoint
            .foreach(qtPoint =>
              if (searchRegion.contains(qtPoint)) {

                //              if (qtPoint.userData.toString().equalsIgnoreCase("Yellow_3_B_467689"))
                //                println()

                val sqDist = Helper.squaredDist(searchRegion.pointCenter.x, searchRegion.pointCenter.y, qtPoint.x, qtPoint.y)

                sortSetSqDist.add(sqDist, qtPoint)

                if (sortSetSqDist.isFull && prevMaxSqrDist != sortSetSqDist.last().distance) {

                  prevMaxSqrDist = sortSetSqDist.last().distance

                  searchRegion.pointHalfXY.x = math.sqrt(prevMaxSqrDist)
                  searchRegion.pointHalfXY.y = searchRegion.pointHalfXY.x
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
    }

    if (quadTreeFirst != null)
      process(true)

    if (quadTreeFirst != quadTreeSecond) {

      lstQT = ListBuffer(quadTreeSecond)
      process(false)
    }
  }

  private def intersects(quadTree: QuadTree, searchRegion: Box) =
    quadTree != null && searchRegion.intersects(quadTree.boundary)

//  private def contains(quadTree: QuadTree, searchPoint: Point) =
//    quadTree != null && quadTree.boundary.contains(searchPoint)

  private def contains(quadTree: QuadTree, searchXY: (Double, Double)) =
    quadTree != null && quadTree.boundary.contains(searchXY._1, searchXY._2)

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

  def findExact(quadTree: QuadTree, searchXY: (Double, Double)): Point = {

    val lstQT = ListBuffer(quadTree)

    lstQT.foreach(qTree => {
      qTree.getLstPoint
        .foreach(qtPoint =>
          if (searchXY._1.equals(qtPoint.x) && searchXY._2.equals(qtPoint.y))
            return qtPoint
        )

      if (contains(qTree.topLeft, searchXY))
        lstQT += qTree.topLeft
      else if (contains(qTree.topRight, searchXY))
        lstQT += qTree.topRight
      else if (contains(qTree.bottomLeft, searchXY))
        lstQT += qTree.bottomLeft
      else if (contains(qTree.bottomRight, searchXY))
        lstQT += qTree.bottomRight
    })

    null
  }

  def spatialIdxRangeLookup(quadTree: QuadTree, searchXY: (Double, Double), k: Int): Set[Int] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    val searchPoint = new Point(searchXY._1, searchXY._2)

    val sPtBestQT = getBestQuadrant(quadTree, searchPoint, k)

    val dim = math.max(math.max(math.abs(searchPoint.x - sPtBestQT.boundary.left), math.abs(searchPoint.x - sPtBestQT.boundary.right)),
      math.max(math.abs(searchPoint.y - sPtBestQT.boundary.bottom), math.abs(searchPoint.y - sPtBestQT.boundary.top)))

    val searchRegion = Box(searchPoint, new Point(dim, dim))

    val sortList = spatialIdxRangeLookupHelper(sPtBestQT, quadTree, searchRegion, k)

    sortList
      .map(f = _.data.userData match {
        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
      })
      .toSet
  }

  private def spatialIdxRangeLookupHelper(quadTreeFirst: QuadTree, quadTreeSecond: QuadTree, searchRegion: Box, k: Int) = {

    //    var totalCount = 0

    val sortList = SortedList[Point](Int.MaxValue, allowDuplicates = true)
    var prevLastElem = sortList.head()
    var currSqDim = math.pow(searchRegion.pointHalfXY.x, 2)
    var weight = 0L

    var lstQT = ListBuffer(quadTreeFirst)

    def getNumPoints(point: Point): Long = point.userData match {
      case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.numPoints
    }

    def process(startRound: Boolean) {
      lstQT.foreach(qTree =>
        if (startRound || qTree != quadTreeFirst) {

          qTree.getLstPoint
            .foreach(qtPoint =>
              if (searchRegion.contains(qtPoint)) {

                //              if (qtPoint.x.toString().startsWith("26157") && qtPoint.y.toString().startsWith("4965"))
                //                print("")

                val sqDist = Helper.squaredDist(searchRegion.pointCenter.x, searchRegion.pointCenter.y, qtPoint.x, qtPoint.y)

                if (prevLastElem == null || sqDist <= currSqDim) {

                  sortList.add(sqDist, qtPoint)

                  weight += getNumPoints(qtPoint)

                  if ((qtPoint != sortList.last().data || prevLastElem == null) && (weight - getNumPoints(sortList.last().data)) >= k) {

                    var elem = sortList.head()
                    weight = getNumPoints(elem.data)

                    while (weight < k) {

                      elem = elem.next

                      weight += getNumPoints(elem.data)
                    }

                    if (elem != prevLastElem) {

                      prevLastElem = elem

                      searchRegion.pointHalfXY.x = math.sqrt(prevLastElem.distance) + expandBy
                      searchRegion.pointHalfXY.y = searchRegion.pointHalfXY.x

                      currSqDim = math.pow(searchRegion.pointHalfXY.x, 2)
                    }

                    if (sortList.last().distance > currSqDim) {

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

    if (quadTreeFirst != quadTreeSecond) {

      lstQT = ListBuffer(quadTreeSecond)
      process(false)
    }

    sortList
  }
}