package org.cusp.bdi.ds

import com.esotericsoftware.kryo.KryoSerializable
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.util.Helper

object SpatialIndex {

  case class KnnLookupInfo(searchPoint: Point, sortSetSqDist: SortedList[Point], rectBestNode: Rectangle) {

    private def dim = if (sortSetSqDist.isFull)
      math.sqrt(sortSetSqDist.last.distance)
    else
      computeDimension(searchPoint, rectBestNode)

    val rectSearchRegion: Rectangle = Rectangle(searchPoint, new Geom2D(dim))
    var prevMaxSqrDist: Double = if (sortSetSqDist.last == null) -1 else sortSetSqDist.last.distance
  }

  def buildRectBounds(iterPoints: Iterable[Point]): Rectangle = {

    val iter = iterPoints.iterator
    val point = iter.next()
    var (minX, minY, maxX, maxY) = (point.x, point.y, point.x, point.y)

    for (point <- iter) {

      if (point.x < minX) minX = point.x
      else if (point.x > maxX) maxX = point.x

      if (point.y < minY) minY = point.y
      else if (point.y > maxY) maxY = point.y
    }

    buildRectBounds((minX, minY), (maxX, maxY))
  }

  def buildRectBounds(mbrEnds: ((Double, Double), (Double, Double))): Rectangle = {

    val halfXY = new Geom2D(((mbrEnds._2._1 - mbrEnds._1._1) + 1) / 2, ((mbrEnds._2._2 - mbrEnds._1._2) + 1) / 2)

    Rectangle(new Geom2D(mbrEnds._1._1 + halfXY.x, mbrEnds._1._2 + halfXY.y), halfXY)
  }

  def computeDimension(searchPoint: Geom2D, rectMBR: Rectangle): Double = {

    //    val dim = math.max(math.max(math.abs(searchPoint.x - rectMBR.left), math.abs(searchPoint.x - rectMBR.right)),
    //      math.max(math.abs(searchPoint.y - rectMBR.bottom), math.abs(searchPoint.y - rectMBR.top))) /*+ errorRange*/

    val left = rectMBR.left
    val bottom = rectMBR.bottom
    val right = rectMBR.right
    val top = rectMBR.top

    math.sqrt(math.max(math.max(Helper.squaredDist(searchPoint.x, searchPoint.y, left, bottom), Helper.squaredDist(searchPoint.x, searchPoint.y, right, bottom)),
      math.max(Helper.squaredDist(searchPoint.x, searchPoint.y, right, top), Helper.squaredDist(searchPoint.x, searchPoint.y, left, top))))
  }

  def testAndAddPoint(point: Point, knnLookupInfo: KnnLookupInfo) {

    val sqDist = Helper.squaredDist(knnLookupInfo.rectSearchRegion.center.x, knnLookupInfo.rectSearchRegion.center.y, point.x, point.y)

    knnLookupInfo.sortSetSqDist.add(sqDist, point)

    if (knnLookupInfo.sortSetSqDist.isFull && knnLookupInfo.prevMaxSqrDist != knnLookupInfo.sortSetSqDist.last.distance) {

      knnLookupInfo.prevMaxSqrDist = knnLookupInfo.sortSetSqDist.last.distance

      knnLookupInfo.rectSearchRegion.halfXY.x = math.sqrt(knnLookupInfo.prevMaxSqrDist)
      knnLookupInfo.rectSearchRegion.halfXY.y = knnLookupInfo.rectSearchRegion.halfXY.x
    }
  }
}

trait SpatialIndex extends KryoSerializable {

  def getTotalPoints: Int

  def insert(iterPoints: Iterator[Point]): Boolean

  def findExact(searchXY: (Double, Double)): Point

  def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point])

  //  def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int): SortedList[Point]
}
