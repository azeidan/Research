package org.cusp.bdi.ds.kt

import org.cusp.bdi.ds.{Point, SpatialIndex}

case class KdTree() extends SpatialIndex {

  override def insert(point: Point) = false

  override def getAllPoints = null

  override def findExact(searchXY: (Double, Double)) = null
}
