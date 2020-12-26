package org.cusp.bdi.ds

import com.esotericsoftware.kryo.KryoSerializable
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.sortset.SortedLinkedList
import org.cusp.bdi.util.Helper
import org.cusp.bdi.util.Helper.FLOAT_ERROR_RANGE

object SpatialIndex extends Serializable {

  //  def computeSquaredDist(manhattanDist: Int) =
  //    2 * math.pow(manhattanDist + 2, 2)

  def maxSquaredEucDist[T <: Geom2D](point: T, rect: Rectangle): Double = {

    val left = rect.left
    val bottom = rect.bottom
    val right = rect.right
    val top = rect.top

    Helper.max(
      Helper.max(Helper.squaredEuclideanDist(point.x, point.y, left, bottom), Helper.squaredEuclideanDist(point.x, point.y, right, bottom)),
      Helper.max(Helper.squaredEuclideanDist(point.x, point.y, left, top), Helper.squaredEuclideanDist(point.x, point.y, left, top))
    ) + FLOAT_ERROR_RANGE
  }

  case class KnnLookupInfo(searchPoint: Point, sortSetSqDist: SortedLinkedList[Point]) {

    var rectSearchRegion: Rectangle = _
    var limitSquaredDist: Double = -1

    def this(searchPoint: Point, sortSetSqDist: SortedLinkedList[Point], rectBestNode: => Rectangle) = {

      this(searchPoint, sortSetSqDist)

      this.limitSquaredDist = if (sortSetSqDist.isFull)
        sortSetSqDist.last.distance
      else
        maxSquaredEucDist(this.searchPoint, rectBestNode)

      this.rectSearchRegion = Rectangle(this.searchPoint, new Geom2D(math.sqrt(this.limitSquaredDist)))
    }
  }

  def buildRectBounds(mbrEnds: ((Double, Double), (Double, Double))): Rectangle = {

    val halfXY = new Geom2D((mbrEnds._2._1 - mbrEnds._1._1) / 2, (mbrEnds._2._2 - mbrEnds._1._2) / 2)

    Rectangle(new Geom2D(mbrEnds._1._1 + halfXY.x, mbrEnds._1._2 + halfXY.y), halfXY)
  }

  def testAndAddPoint(point: Point, knnLookupInfo: KnnLookupInfo) {

    //    if (knnLookupInfo.searchPoint.userData.toString.equalsIgnoreCase("bus_1_a_855565") &&
    //      (point.userData.toString.equalsIgnoreCase("bus_1_b_650832") || point.userData.toString.equalsIgnoreCase("bus_1_b_291848 ")))
    //      println

    val sqDist = Helper.squaredEuclideanDist(knnLookupInfo.rectSearchRegion.center.x, knnLookupInfo.rectSearchRegion.center.y, point.x, point.y)

    knnLookupInfo.sortSetSqDist.add(sqDist, point)

    if (knnLookupInfo.sortSetSqDist.isFull && knnLookupInfo.limitSquaredDist != knnLookupInfo.sortSetSqDist.last.distance) {

      knnLookupInfo.limitSquaredDist = knnLookupInfo.sortSetSqDist.last.distance + FLOAT_ERROR_RANGE // double precision errors

      knnLookupInfo.rectSearchRegion.halfXY.x = math.sqrt(knnLookupInfo.limitSquaredDist)
      knnLookupInfo.rectSearchRegion.halfXY.y = knnLookupInfo.rectSearchRegion.halfXY.x
    }
  }
}

trait SpatialIndex extends KryoSerializable {

  def dummyNode: AnyRef

  def estimateNodeCount(pointCount: Long): Int

  def getTotalPoints: Int

  @throws(classOf[IllegalStateException])
  def insert(rectBounds: Rectangle, iterPoints: Iterator[Point], histogramBarWidth: Int): Boolean

  def findExact(searchXY: (Double, Double)): Point

  def allPoints: Iterator[Iterator[Point]]

  def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedLinkedList[Point])
}
