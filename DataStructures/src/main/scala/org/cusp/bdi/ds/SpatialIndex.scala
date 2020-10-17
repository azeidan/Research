package org.cusp.bdi.ds

import com.esotericsoftware.kryo.KryoSerializable
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.util.Helper

import scala.collection.mutable.ListBuffer

object SpatialIndex {

  //  val MAX_SEARCH_REGION_DIM = math.sqrt(Double.MaxValue)

  def calcListInfo(iterPoints: Iterable[Point]): (Int, Rectangle) = {

    val iter = iterPoints.iterator
    val point = iter.next()
    var size = 1
    var (minX, minY, maxX, maxY) = (point.x, point.y, point.x, point.y)

    for (point <- iter) {

      size += 1

      if (point.x < minX) minX = point.x
      else if (point.x > maxX) maxX = point.x

      if (point.y < minY) minY = point.y
      else if (point.y > maxY) maxY = point.y
    }

    (size, buildRectBounds((minX, minY), (maxX, maxY)))
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

  def testAndAddPoint(point: Point, rectSearchRegion: Rectangle, sortSetSqDist: SortedList[Point], currMaxSqrDist: Double): Double = {

    val sqDist = Helper.squaredDist(rectSearchRegion.center.x, rectSearchRegion.center.y, point.x, point.y)

    sortSetSqDist.add(sqDist, point)

    var prevMaxSqrDist = currMaxSqrDist

    if (sortSetSqDist.isFull && prevMaxSqrDist != sortSetSqDist.last.distance) {

      prevMaxSqrDist = sortSetSqDist.last.distance

      rectSearchRegion.halfXY.x = math.sqrt(prevMaxSqrDist)
      rectSearchRegion.halfXY.y = rectSearchRegion.halfXY.x
    }

    prevMaxSqrDist
  }
}

trait SpatialIndex extends KryoSerializable {

  def getTotalPoints: Int

  def insert(iterPoints: ListBuffer[Point]): Boolean

  def findExact(searchXY: (Double, Double)): Point

  def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point])

  //  def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int): SortedList[Point]
}
