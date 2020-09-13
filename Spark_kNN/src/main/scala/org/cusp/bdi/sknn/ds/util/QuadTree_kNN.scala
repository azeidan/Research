package org.cusp.bdi.sknn.ds.util

import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.{Point, PointBase, Rectangle}
import org.cusp.bdi.sknn.GlobalIndexPointData
import org.cusp.bdi.sknn.ds.util.QuadTree_kNN.expandBy
import org.cusp.bdi.util.{Helper, SortedList}

import scala.collection.mutable.ListBuffer

object QuadTree_kNN {

  private val expandBy = math.sqrt(8)
}

class QuadTree_kNN(_boundary: Rectangle) extends QuadTree(_boundary) with SpatialIndex_kNN {

  //  override def getDepth: Long = depth

  def this(leftBot: (Double, Double), rightTop: (Double, Double)) {

    this(null)

    val pointHalfXY = new PointBase(((rightTop._1 - leftBot._1) + 1) / 2.0, ((rightTop._2 - leftBot._2) + 1) / 2.0)

    this.boundary = Rectangle(new PointBase(pointHalfXY.x + leftBot._1, pointHalfXY.y + leftBot._2), pointHalfXY)
  }

  def this(mbr: (Double, Double, Double, Double), gridSquareLen: Double) {

    this(null)

    val minX = mbr._1 * gridSquareLen
    val minY = mbr._2 * gridSquareLen
    val maxX = mbr._3 * gridSquareLen + gridSquareLen
    val maxY = mbr._4 * gridSquareLen + gridSquareLen

    val halfWidth = (maxX - minX) / 2
    val halfHeight = (maxY - minY) / 2

    this.boundary = Rectangle(new PointBase(halfWidth + minX, halfHeight + minY), new PointBase(halfWidth, halfHeight))
  }

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int) {

    //    if (searchPoint.userData.toString().equalsIgnoreCase("yellow_3_a_772558"))
    //      println

    var searchRegion: Rectangle = null

    var sPtBestQT: QuadTree = null

    var dim = 0.0

    if (sortSetSqDist.isFull)
      dim = math.sqrt(sortSetSqDist.last.distance)
    else {

      sPtBestQT = getBestQuadrant(searchPoint, k)

      //      val dim = math.ceil(math.sqrt(getFurthestCorner(searchPoint, sPtBestQT)._1))

      dim = math.max(math.max(math.abs(searchPoint.x - sPtBestQT.boundary.left), math.abs(searchPoint.x - sPtBestQT.boundary.right)),
        math.max(math.abs(searchPoint.y - sPtBestQT.boundary.bottom), math.abs(searchPoint.y - sPtBestQT.boundary.top)))
    }

    searchRegion = Rectangle(searchPoint, new PointBase(dim, dim))

    pointsWithinRegion(sPtBestQT, searchRegion, sortSetSqDist)
  }

  private def pointsWithinRegion(sPtBestQT: QuadTree, searchRegion: Rectangle, sortSetSqDist: SortedList[Point]) {

    var prevMaxSqrDist = if (sortSetSqDist.last == null) -1 else sortSetSqDist.last.distance

    def process(rootQT: QuadTree, skipQT: QuadTree) {

      var lstQT = ListBuffer(rootQT)

      lstQT.foreach(qTree =>
        if (qTree != skipQT) {

          qTree.getLstPoint
            .foreach(qtPoint =>
              if (searchRegion.contains(qtPoint)) {

                //              if (qtPoint.userData.toString().equalsIgnoreCase("Yellow_3_B_467689"))
                //                println()

                val sqDist = Helper.squaredDist(searchRegion.pointCenter.x, searchRegion.pointCenter.y, qtPoint.x, qtPoint.y)

                sortSetSqDist.add(sqDist, qtPoint)

                if (sortSetSqDist.isFull && prevMaxSqrDist != sortSetSqDist.last.distance) {

                  prevMaxSqrDist = sortSetSqDist.last.distance

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

    if (sPtBestQT != null)
      process(sPtBestQT, null)

    if (sPtBestQT != this)
      process(this, sPtBestQT)
  }

  private def intersects(quadTree: QuadTree, searchRegion: Rectangle) =
    quadTree != null && searchRegion.intersects(quadTree.boundary)

  private def getBestQuadrant(searchPoint: PointBase, k: Int): QuadTree = {

    // find leaf containing point
    var qTree: QuadTree = this

    def testQuad(qtQuad: QuadTree) =
      qtQuad != null && qtQuad.getTotalPoints >= k && qtQuad.boundary.contains(searchPoint.x, searchPoint.y)

    while (true)
      if (testQuad(qTree.topLeft))
        qTree = qTree.topLeft
      else if (testQuad(qTree.topRight))
        qTree = qTree.topRight
      else if (testQuad(qTree.bottomLeft))
        qTree = qTree.bottomLeft
      else if (testQuad(qTree.bottomRight))
        qTree = qTree.bottomRight
      else
        return qTree

    null
  }

  override def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int): Set[Int] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    val searchPoint = new PointBase(searchXY._1, searchXY._2)

    val sPtBestQT = getBestQuadrant(searchPoint, k)

    val dim = math.max(math.max(math.abs(searchPoint.x - sPtBestQT.boundary.left), math.abs(searchPoint.x - sPtBestQT.boundary.right)),
      math.max(math.abs(searchPoint.y - sPtBestQT.boundary.bottom), math.abs(searchPoint.y - sPtBestQT.boundary.top)))

    val searchRegion = Rectangle(searchPoint, new PointBase(dim, dim))

    val sortList = spatialIdxRangeLookupHelper(sPtBestQT, searchRegion, k)

    sortList
      .map(_.data.userData match {
        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
      })
      .toSet
  }

  private def spatialIdxRangeLookupHelper(sPtBestQT: QuadTree, searchRegion: Rectangle, k: Int) = {

    val sortList = SortedList[Point](Int.MaxValue)
    var limitNode = sortList.head
    var currSqDim = math.pow(searchRegion.pointHalfXY.x, 2)
    var weight = 0L


    def getNumPoints(point: Point): Long = point.userData match {
      case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.numPoints
    }

    def process(rootQT: QuadTree, skipQT: QuadTree) {

      var lstQT = ListBuffer(rootQT)

      lstQT.foreach(qTree =>
        if (qTree != skipQT) {

          qTree.getLstPoint
            .foreach(qtPoint =>
              if (searchRegion.contains(qtPoint)) {

                //              if (qtPoint.x.toString().startsWith("26157") && qtPoint.y.toString().startsWith("4965"))
                //                print("")

                val sqDistQTPoint = Helper.squaredDist(searchRegion.pointCenter.x, searchRegion.pointCenter.y, qtPoint.x, qtPoint.y)

                // add point if it's within the search radius
                if (limitNode == null || sqDistQTPoint < currSqDim) {

                  sortList.add(sqDistQTPoint, qtPoint)

                  weight += getNumPoints(qtPoint)

                  // see if region can shrink if at least the last node can be dropped
                  if ((limitNode == null || sortList.last.data != qtPoint) && (weight - getNumPoints(sortList.last.data)) >= k) {

                    var elem = sortList.head
                    var newWeight = getNumPoints(elem.data)

                    while (newWeight < k) {

                      elem = elem.next
                      newWeight += getNumPoints(elem.data)
                    }

                    if (limitNode != elem) {

                      limitNode = elem

                      searchRegion.pointHalfXY.x = math.sqrt(limitNode.distance) + expandBy
                      searchRegion.pointHalfXY.y = searchRegion.pointHalfXY.x

                      currSqDim = math.pow(searchRegion.pointHalfXY.x, 2)

                      while (elem.next != null && elem.next.distance < currSqDim) {

                        elem = elem.next
                        newWeight += getNumPoints(elem.data)
                      }

                      sortList.stopAt(elem)
                      weight = newWeight
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

    process(sPtBestQT, null)

    if (sPtBestQT != this)
      process(this, sPtBestQT)

    sortList
  }
}