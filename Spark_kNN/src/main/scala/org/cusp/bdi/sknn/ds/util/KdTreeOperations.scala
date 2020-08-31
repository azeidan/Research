package org.cusp.bdi.sknn.ds.util

import org.cusp.bdi.ds.{Point, SpatialIndex}
import org.cusp.bdi.util.SortedList

object KdTreeOperations extends SpatialIndexOperations {

  override def nearestNeighbor(spatialIndex: SpatialIndex, searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int): Unit = {}

  override def spatialIdxRangeLookup(spatialIndex: SpatialIndex, searchXY: (Double, Double), k: Int) = null
}