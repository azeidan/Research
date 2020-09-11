package org.cusp.bdi.sknn.ds.util

import org.cusp.bdi.ds.Point
import org.cusp.bdi.util.SortedList

import scala.collection.mutable.ListBuffer

trait SpatialIndex_kNN extends Serializable {

  def getTotalPoints: Long

  //
  //  def getDepth: Long
  //

  def insert(point: Point): Boolean

  def getAllPoints: ListBuffer[ListBuffer[Point]]

  def findExact(searchXY: (Double, Double)): Point

  def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int)

  def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int): Set[Int]
}
