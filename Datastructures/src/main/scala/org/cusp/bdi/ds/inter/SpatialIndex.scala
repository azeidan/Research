package org.cusp.bdi.ds.inter

import org.cusp.bdi.ds.Point

import scala.collection.mutable.ListBuffer

trait SpatialIndex extends Serializable {

  def getTotalPoints: Long

  def getLstPoint: ListBuffer[Point]

  def insert(lstPoint: List[Point]): Unit

  def insert(point: Point): Boolean

  def getAllPoints: ListBuffer[ListBuffer[Point]]
}
