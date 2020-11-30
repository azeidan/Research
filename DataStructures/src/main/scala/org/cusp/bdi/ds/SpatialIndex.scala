package org.cusp.bdi.ds

import com.esotericsoftware.kryo.KryoSerializable
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.KdTree
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.sortset.SortedList
import org.cusp.bdi.util.Helper

import scala.collection.immutable.Iterable

object TypeSpatialIndex extends Enumeration with Serializable {

  val quadTree: TypeSpatialIndex.Value = Value(0.toByte)
  val kdTree: TypeSpatialIndex.Value = Value(1.toByte)
}

object SpatialIndex {

  def apply(typeSpatialIndex: TypeSpatialIndex.Value): SpatialIndex =
    typeSpatialIndex match {
      case TypeSpatialIndex.quadTree => new QuadTree()
      case TypeSpatialIndex.kdTree => new KdTree()
      case _ => throw new IllegalArgumentException("Unsupported Spatial Index Type: " + typeSpatialIndex)
    }

  case class KnnLookupInfo(searchPoint: Point, sortSetSqDist: SortedList[Point]) {

    var rectSearchRegion: Rectangle = _
    var prevMaxSqrDist: Double = -1

    def this(searchPoint: Point, sortSetSqDist: SortedList[Point], rectBestNode: => Rectangle) = {

      this(searchPoint, sortSetSqDist)

      def dim = if (sortSetSqDist.isFull) math.sqrt(sortSetSqDist.last.distance)
      else computeDimension(this.searchPoint, rectBestNode)

      this.rectSearchRegion = Rectangle(this.searchPoint, new Geom2D(dim))
      this.prevMaxSqrDist = if (sortSetSqDist.last == null) -1
      else sortSetSqDist.last.distance
    }
  }

  def buildRectBounds(mbrEnds: ((Double, Double), (Double, Double))): Rectangle = {

    val halfXY = new Geom2D((mbrEnds._2._1 - mbrEnds._1._1) / 2, (mbrEnds._2._2 - mbrEnds._1._2) / 2)

    Rectangle(new Geom2D(mbrEnds._1._1 + halfXY.x, mbrEnds._1._2 + halfXY.y), halfXY)
  }

  def computeDimension(searchPoint: Geom2D, rectMBR: Rectangle): Double = {

    val left = rectMBR.left
    val bottom = rectMBR.bottom
    val right = rectMBR.right
    val top = rectMBR.top

    math.sqrt(Helper.squaredEuclideanDist(searchPoint.x, searchPoint.y, left, bottom).max(Helper.squaredEuclideanDist(searchPoint.x, searchPoint.y, right, bottom))
      .max(Helper.squaredEuclideanDist(searchPoint.x, searchPoint.y, right, top).max(Helper.squaredEuclideanDist(searchPoint.x, searchPoint.y, left, top))))
  }

  def testAndAddPoint(point: Point, knnLookupInfo: KnnLookupInfo) {

    val sqDist = Helper.squaredEuclideanDist(knnLookupInfo.rectSearchRegion.center.x, knnLookupInfo.rectSearchRegion.center.y, point.x, point.y)

    knnLookupInfo.sortSetSqDist.add(sqDist, point)

    if (knnLookupInfo.sortSetSqDist.isFull && knnLookupInfo.prevMaxSqrDist != knnLookupInfo.sortSetSqDist.last.distance) {

      knnLookupInfo.prevMaxSqrDist = knnLookupInfo.sortSetSqDist.last.distance

      knnLookupInfo.rectSearchRegion.halfXY.x = math.sqrt(knnLookupInfo.prevMaxSqrDist)
      knnLookupInfo.rectSearchRegion.halfXY.y = knnLookupInfo.rectSearchRegion.halfXY.x
    }
  }
}

trait SpatialIndex extends KryoSerializable with Iterable[Iterator[Point]] {

  def getTotalPoints: Int

  @throws(classOf[IllegalStateException])
  def insert(rectBounds: Rectangle, iterPoints: Iterator[Point], otherInitializers: Any*): Boolean

  def findExact(searchXY: (Double, Double)): Point

  def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point])
}
