package org.cusp.bdi.ds

import com.esotericsoftware.kryo.KryoSerializable
import org.cusp.bdi.ds.geom.{Point, Rectangle}
import org.cusp.bdi.util.Helper

protected class SearchRegionInfo(_limitNode: Node[Point], _sqDim: Double) {

  var limitNode: Node[Point] = _limitNode
  var sqDim: Double = _sqDim
  var weight: Long = 0L
}

protected class DoubleWrapper(_d: Double) {
  var d: Double = _d
}

trait PointData extends Serializable {

  def numPoints: Int

  def equals(other: Any): Boolean
}

object SpatialIndex {

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

  def updateMatchListAndRegion(point: Point, rectSearchRegion: Rectangle, sortList: SortedList[Point], k: Int, searchRegionInfo: SearchRegionInfo): Unit = {

    def getNumPoints(point: Point): Long = point.userData match {
      case pointData: PointData => pointData.numPoints
      case _ => throw new ClassCastException("Point userdata must extend " + classOf[PointData].getName)
    }

    if (rectSearchRegion.contains(point)) {

      //              if (qtPoint.x.toString().startsWith("26157") && qtPoint.y.toString().startsWith("4965"))
      //                print("")

      val sqDistQTPoint = Helper.squaredDist(rectSearchRegion.center.x, rectSearchRegion.center.y, point.x, point.y)

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

            rectSearchRegion.halfXY.x = math.sqrt(searchRegionInfo.limitNode.distance) + expandBy
            rectSearchRegion.halfXY.y = rectSearchRegion.halfXY.x

            searchRegionInfo.sqDim = math.pow(rectSearchRegion.halfXY.x, 2)

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

trait SpatialIndex extends KryoSerializable {

  def getTotalPoints: Int

  def insert(iterPoints: Iterator[Point]): Boolean

  def findExact(searchXY: (Double, Double)): Point

  def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int)

  def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int): SortedList[Point]
}
