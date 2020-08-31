package org.cusp.bdi.ds

import scala.collection.mutable.ListBuffer

trait SpatialIndex extends Serializable {

  def insert(point: Point): Boolean

  def getAllPoints: ListBuffer[ListBuffer[Point]]

  def findExact(searchXY: (Double, Double)): Point
}
