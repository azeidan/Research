package org.cusp.bdi.sknn.ds.util

import org.apache.spark.serializer.KryoSerializer
import org.cusp.bdi.ds.{Point, Rectangle}
import org.cusp.bdi.sknn.GlobalIndexPointData
import org.cusp.bdi.util.{Helper, Node, SortedList}

import scala.collection.mutable.ListBuffer

class SearchRegionInfo(_limitNode: Node[Point], _sqDim: Double) {
  var limitNode: Node[Point] = _limitNode
  var sqDim: Double = _sqDim
  var weight: Long = 0L
}

class DoubleWrapper(_d: Double) {
  var d = _d
}

object SpatialIndex_kNN {

  val expandBy: Double = math.sqrt(8)

  def testAndAddPoint(kdtPoint: Point, searchRegion: Rectangle, sortSetSqDist: SortedList[Point], prevMaxSqrDist: DoubleWrapper): Unit = {

    val sqDist = Helper.squaredDist(searchRegion.center.x, searchRegion.center.y, kdtPoint.x, kdtPoint.y)

    sortSetSqDist.add(sqDist, kdtPoint)

    if (sortSetSqDist.isFull && prevMaxSqrDist.d != sortSetSqDist.last.distance) {

      prevMaxSqrDist.d = sortSetSqDist.last.distance

      searchRegion.halfXY.x = math.sqrt(prevMaxSqrDist.d)
      searchRegion.halfXY.y = searchRegion.halfXY.x
    }
  }

  def updateMatchListAndRegion(point: Point, searchRegion: Rectangle, sortList: SortedList[Point], k: Int, searchRegionInfo: SearchRegionInfo): Unit = {

    def getNumPoints(point: Point): Long = point.userData match {
      case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.numPoints
    }

    if (searchRegion.contains(point)) {

      //              if (qtPoint.x.toString().startsWith("26157") && qtPoint.y.toString().startsWith("4965"))
      //                print("")

      val sqDistQTPoint = Helper.squaredDist(searchRegion.center.x, searchRegion.center.y, point.x, point.y)

      // add point if it's within the search radius
      if (searchRegionInfo.limitNode == null || sqDistQTPoint < searchRegionInfo.sqDim) {

        sortList.add(sqDistQTPoint, point)

        searchRegionInfo.weight += getNumPoints(point)

        // see if region can shrink if at least the last node can be dropped
        if ((searchRegionInfo.limitNode == null || sortList.last.data != point) && (searchRegionInfo.weight - getNumPoints(sortList.last.data)) >= k) {

          var elem = sortList.head
          var newWeight = getNumPoints(elem.data)

          while (newWeight < k) {

            elem = elem.next
            newWeight += getNumPoints(elem.data)
          }

          if (searchRegionInfo.limitNode != elem) {

            searchRegionInfo.limitNode = elem

            searchRegion.halfXY.x = math.sqrt(searchRegionInfo.limitNode.distance) + expandBy
            searchRegion.halfXY.y = searchRegion.halfXY.x

            searchRegionInfo.sqDim = math.pow(searchRegion.halfXY.x, 2)

            while (elem.next != null && elem.next.distance < searchRegionInfo.sqDim) {

              elem = elem.next
              newWeight += getNumPoints(elem.data)
            }

            sortList.stopAt(elem)
            searchRegionInfo.weight = newWeight
          }
        }
      }
    }
  }
}

trait SpatialIndex_kNN extends Serializable {

  def getTotalPoints: Int

  def insert(iterPoints: Iterator[Point]): Boolean

  def findExact(searchXY: (Double, Double)): Point

  def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int)

  def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int): Set[Int]
}
