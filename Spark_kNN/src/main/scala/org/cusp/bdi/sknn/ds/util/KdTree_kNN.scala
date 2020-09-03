package org.cusp.bdi.sknn.ds.util

import org.cusp.bdi.ds.Point
import org.cusp.bdi.ds.kt.KdTree
import org.cusp.bdi.util.SortedList

class KdTree_kNN() extends KdTree with SpatialIndex_kNN {

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int): Unit = {}

  override def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int) = null

  override def getTotalPoints = -1

  override def insert(point: Point) = false

  override def getAllPoints = null

  override def findExact(searchXY: (Double, Double)) = null
}