package org.cusp.bdi.sknn.ds.util

import org.cusp.bdi.ds.{Point, SpatialIndex}
import org.cusp.bdi.util.SortedList

trait SpatialIndexOperations extends Serializable {

  def nearestNeighbor(spatialIndex: SpatialIndex, searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int)

  def spatialIdxRangeLookup(spatialIndex: SpatialIndex, searchXY: (Double, Double), k: Int): Set[Int]
}
