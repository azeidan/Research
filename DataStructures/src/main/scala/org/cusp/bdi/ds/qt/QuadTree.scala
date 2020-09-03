package org.cusp.bdi.ds.qt

import org.cusp.bdi.ds.qt.QuadTree.capacity
import org.cusp.bdi.ds.{Box, Point}

import scala.collection.mutable.ListBuffer

object QuadTree extends Serializable {

  val capacity = 4
}

class QuadTree extends Serializable {

  private var totalPoints = 0L
  private val points = ListBuffer[Point]()

  var boundary: Box = _
  var topLeft: QuadTree = _
  var topRight: QuadTree = _
  var bottomLeft: QuadTree = _
  var bottomRight: QuadTree = _

  def this(boundary: Box) = {
    this()
    this.boundary = boundary
  }

  //    var parent: QuadTree = null

  def getTotalPoints: Long = totalPoints

  def getLstPoint: ListBuffer[Point] = points

  def findExact(searchXY: (Double, Double)): Point = {

    val lstQT = ListBuffer(this)

    lstQT.foreach(qTree => {
      qTree.getLstPoint
        .foreach(qtPoint =>
          if (searchXY._1.equals(qtPoint.x) && searchXY._2.equals(qtPoint.y))
            return qtPoint
        )

      if (contains(qTree.topLeft, searchXY))
        lstQT += qTree.topLeft
      else if (contains(qTree.topRight, searchXY))
        lstQT += qTree.topRight
      else if (contains(qTree.bottomLeft, searchXY))
        lstQT += qTree.bottomLeft
      else if (contains(qTree.bottomRight, searchXY))
        lstQT += qTree.bottomRight
    })

    null
  }

  def insert(point: Point): Boolean = {
    if (!insertPoint(point))
      throw new Exception("Point insert failed: %s in QuadTree: %s".format(point, this))

    true
  }

  private def contains(quadTree: QuadTree, searchXY: (Double, Double)) =
    quadTree != null && quadTree.boundary.contains(searchXY._1, searchXY._2)

  private def insertPoint(point: Point): Boolean = {

    var qTree = this

    if (this.boundary.contains(point))
      while (true) {

        qTree.totalPoints += 1

        if (qTree.points.size < capacity) {

          qTree.points += point
          return true
        }
        else
        // switch to proper quadrant?
          qTree = if (point.x <= qTree.boundary.pointCenter.x)
            if (point.y >= qTree.boundary.pointCenter.y) {

              if (qTree.topLeft == null)
                qTree.topLeft = new QuadTree(qTree.boundary.topLeftQuadrant /*, qTree*/)

              qTree.topLeft
            }
            else {

              if (qTree.bottomLeft == null)
                qTree.bottomLeft = new QuadTree(qTree.boundary.bottomLeftQuadrant /*, qTree*/)

              qTree.bottomLeft
            }
          else if (point.y >= qTree.boundary.pointCenter.y) {

            if (qTree.topRight == null)
              qTree.topRight = new QuadTree(qTree.boundary.topRightQuadrant /*, qTree*/)

            qTree.topRight
          }
          else {

            if (qTree.bottomRight == null)
              qTree.bottomRight = new QuadTree(qTree.boundary.bottomRightQuadrant /*, qTree*/)

            qTree.bottomRight
          }
      }

    false
  }

  def getAllPoints: ListBuffer[ListBuffer[Point]] = {

    val lstQT = ListBuffer(this)

    lstQT.map(qTree => {

      if (qTree.topLeft != null) lstQT += qTree.topLeft
      if (qTree.topRight != null) lstQT += qTree.topRight
      if (qTree.bottomLeft != null) lstQT += qTree.bottomLeft
      if (qTree.bottomRight != null) lstQT += qTree.bottomRight
    })

    lstQT.map(_.points) //.flatMap(_.seq)
  }

  override def toString: String =
    "%s\t%d\t%d".format(boundary, points.size, totalPoints)
}
