package org.cusp.bdi.sknn.ds.util

import org.cusp.bdi.ds.Point
import org.cusp.bdi.ds.kt.KdTree
import org.cusp.bdi.util.SortedList

import scala.collection.mutable.ListBuffer

class KdTree_kNN() extends KdTree with SpatialIndex_kNN {

//  override def getDepth: Long = 0

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int): Unit = {}

  override def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int): Set[Int] = null

  override def getTotalPoints: Long = -1

  override def insert(point: Point) = false

  override def getAllPoints: ListBuffer[ListBuffer[Point]] = null

  override def findExact(searchXY: (Double, Double)): Point = null
}