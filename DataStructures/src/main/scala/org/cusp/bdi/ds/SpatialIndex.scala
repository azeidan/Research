package org.cusp.bdi.ds

import com.esotericsoftware.kryo.KryoSerializable
import org.cusp.bdi.ds.geom.{Point, Rectangle}
import org.cusp.bdi.util.Helper

object SpatialIndex {

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

  def insert(iterPoints: Iterator[Point]): Boolean

  def findExact(searchXY: (Double, Double)): Point

  def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point])

  //  def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int): SortedList[Point]
}
