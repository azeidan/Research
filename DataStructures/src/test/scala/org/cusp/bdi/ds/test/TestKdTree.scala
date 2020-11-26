package org.cusp.bdi.ds.test

import org.cusp.bdi.ds.SpatialIndex
import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds.geom.Point
import org.cusp.bdi.ds.kdt.KdTree

import scala.collection.mutable.ListBuffer

object TestKdTree {

  def main(args: Array[String]): Unit = {

    val lstPoints = ListBuffer(new Point(3119.0000000000000000000000, 719),
      new Point(3119.0000000000000000000000, 721),
      new Point(3119.0000000000000000000000, 722),
      new Point(3119.0000000000000000000000, 723),
      new Point(3119.0000000000000000000000, 724),
      new Point(3119.0000000000000000000000, 725),
      new Point(3119.0000000000000000000000, 726),
      new Point(3119.0000000000000000000000, 727),
      new Point(3119.0000000000000000000000, 728),
      new Point(3119.0000000000000000000000, 730),
      new Point(3119.0000000000000000000000, 731),
      new Point(3119.0000000000000000000000, 732),
      new Point(3119.0000000000000000000000, 734),
      new Point(3120.0000000000000000000000, 720),
      new Point(3120.0000000000000000000000, 722),
      new Point(3120.0000000000000000000000, 723),
      new Point(3120.0000000000000000000000, 724),
      new Point(3120.0000000000000000000000, 725),
      new Point(3120.0000000000000000000000, 732),
      new Point(3120.0000000000000000000000, 732),
      new Point(3120.0000000000000000000000, 732),
      new Point(3120.0000000000000000000000, 732),
      new Point(3120.0000000000000000000000, 732))

    val minX = lstPoints.minBy(_.x).x
    val maxX = lstPoints.maxBy(_.x).x
    val minY = lstPoints.minBy(_.y).y
    val maxY = lstPoints.maxBy(_.y).y

    val kdt = new KdTree(SpatialIndex.buildRectBounds(((minX, minY), (maxX, maxY))), 1)

    kdt.insert(lstPoints.iterator)

    val pt = kdt.findExact((3119.0000000000000000000000, 734))

    println(pt)
  }
}