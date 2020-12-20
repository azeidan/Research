package org.cusp.bdi.ds

import com.esotericsoftware.kryo.KryoSerializable
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.sortset.SortedLinkedList
import org.cusp.bdi.util.Helper

object SpatialIndex extends Serializable {

  def computeSquaredDist(manhattanDist: Int) =
    2 * math.pow(manhattanDist + 2, 2)

  case class KnnLookupInfo(searchPoint: Point, sortSetSqDist: SortedLinkedList[Point]) {

    var rectSearchRegion: Rectangle = _
    var limitDistSquared: Double = -1

    def this(searchPoint: Point, sortSetSqDist: SortedLinkedList[Point], rectBestNode: => Rectangle) = {

      this(searchPoint, sortSetSqDist)

      //      ?????? -> change dim to use technique for lookup sqrt(2) * max manhattan dist
      //      def dim = if (sortSetSqDist.isFull) {
      //
      //        math.sqrt(sortSetSqDist.last.distance)
      //      }
      //      else
      //        rectBestNode.maxManhattanDist(this.searchPoint)

      this.limitDistSquared = if (sortSetSqDist.isEmpty)
        computeSquaredDist(rectBestNode.maxManhattanDist(this.searchPoint))
      else
        computeSquaredDist(Helper.manhattanDist(searchPoint.x, searchPoint.y, sortSetSqDist.last.data.x, sortSetSqDist.last.data.y))

      this.rectSearchRegion = Rectangle(this.searchPoint, new Geom2D(math.sqrt(this.limitDistSquared)))
    }
  }

  def buildRectBounds(mbrEnds: ((Double, Double), (Double, Double))): Rectangle = {

    val halfXY = new Geom2D((mbrEnds._2._1 - mbrEnds._1._1) / 2, (mbrEnds._2._2 - mbrEnds._1._2) / 2)

    Rectangle(new Geom2D(mbrEnds._1._1 + halfXY.x, mbrEnds._1._2 + halfXY.y), halfXY)
  }

  def testAndAddPoint(point: Point, knnLookupInfo: KnnLookupInfo) {

    //    if (knnLookupInfo.searchPoint.userData.toString.equalsIgnoreCase("bus_3_b_780100") && point.userData.toString.equalsIgnoreCase("bus_3_a_893929"))
    //    println

    val sqDist = Helper.squaredEuclideanDist(knnLookupInfo.rectSearchRegion.center.x, knnLookupInfo.rectSearchRegion.center.y, point.x, point.y)

    knnLookupInfo.sortSetSqDist.add(sqDist, point)

    if (knnLookupInfo.sortSetSqDist.isFull && knnLookupInfo.limitDistSquared != knnLookupInfo.sortSetSqDist.last.distance) {

      knnLookupInfo.limitDistSquared = knnLookupInfo.sortSetSqDist.last.distance

      knnLookupInfo.rectSearchRegion.halfXY.x = math.sqrt(knnLookupInfo.limitDistSquared)
      knnLookupInfo.rectSearchRegion.halfXY.y = knnLookupInfo.rectSearchRegion.halfXY.x
    }
  }
}

trait SpatialIndex extends KryoSerializable {

  def dummyNode: AnyRef

  def estimateNodeCount(pointCount: Long): Int

  def getTotalPoints: Int

  @throws(classOf[IllegalStateException])
  def insert(rectBounds: Rectangle, iterPoints: Iterator[Point], otherInitializers: Any*): Boolean

  def findExact(searchXY: (Double, Double)): Point

  def allPoints: Iterator[Iterator[Point]]

  def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedLinkedList[Point])
}
