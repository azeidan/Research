package org.cusp.bdi.ds.test

import org.cusp.bdi.ds.KdTree
import org.cusp.bdi.ds.geom.Point

import scala.collection.mutable.ListBuffer

object TestKdTree {

  def main(args: Array[String]): Unit = {

    val lstPoint = ListBuffer(new Point(0.54, 0.93), new Point(0.96, 0.86), new Point(0.42, 0.67), new Point(0.11, 0.53), new Point(0.64, 0.29), new Point(0.27, 0.75), new Point(0.81, 0.63))

    val kdt = new KdTree()
    kdt.insert(lstPoint.iterator)
    kdt.printInOrder()

    println(kdt.findExact((9, 1)))
  }
}