package org.cusp.bdi.ds.test

import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds.geom.Point
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.sortset.SortedLinkedList

import scala.io.Source

object TestKdTree {

  def main(args: Array[String]): Unit = {

    val qt = new QuadTree

    val points = Source.fromFile("/media/cusp/Data/GeoMatch_Files/InputFiles/RandomSamples_OLD/Bus_1_A.csv")
      .getLines()
      .map(_.split(","))
      .map(arr => new Point(arr(1).toDouble, arr(2).toDouble, arr(0)))
      .toList

    var (minX, minY, maxX, maxY) = (Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue)

    points.foreach(pt => {

      if (pt.x < minX) minX = pt.x
      if (pt.x > maxX) maxX = pt.x

      if (pt.y < minY) minY = pt.y
      if (pt.y > maxY) maxY = pt.y
    })

    minX = minX.floor
    minY = minY.floor
    maxX = maxX.ceil
    maxY = maxY.ceil

    qt.insert(buildRectBounds(minX, minY, maxX, maxY), points.iterator, 1)

    println(qt)

    var counter = 1

    qt.nearestNeighbor(new Point(914077.4375*2, 122589.3594*2), new SortedLinkedList[Point](10))

    Source.fromFile("/media/cusp/Data/GeoMatch_Files/InputFiles/RandomSamples_OLD/Bus_1_B.csv")
      .getLines()
      .map(_.split(","))
      .map(arr => new Point(arr(1).toDouble+914077, arr(2).toDouble+122589, arr(0)))
      .foreach(pt => {

        val sortSetSqDist = new SortedLinkedList[Point](3)
        counter += 1
        qt.nearestNeighbor(pt, sortSetSqDist)
        if (counter % 1000 == 0)
          println(">> " + counter)
      })
  }
}