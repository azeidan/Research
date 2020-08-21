package org.cusp.bdi.ds.qt

import org.cusp.bdi.ds.qt.QuadTree.capacity
import org.cusp.bdi.ds.{Box, Point}

import scala.collection.mutable.ListBuffer

object QuadTree extends Serializable {

  val capacity = 4
}

case class QuadTree(boundary: Box) extends Serializable {

  private val points = ListBuffer[Point]()
  var topLeft: QuadTree = _
  var topRight: QuadTree = _
  var bottomLeft: QuadTree = _
  var bottomRight: QuadTree = _
  private var totalPoints = 0L

  //    var parent: QuadTree = null

  def getTotalPoints: Long = totalPoints

  def getLstPoint: ListBuffer[Point] = points

  def getMBR: (Double, Double, Double, Double) = (boundary.left, boundary.bottom, boundary.right, boundary.top)

  //    def this(boundary: Box, parent: QuadTree) = {
  //
  //        this(boundary)
  //        this.parent = parent
  //    }

  def insert(lstPoint: List[Point]): Unit =
    lstPoint.foreach(insert)

  def insert(point: Point): Boolean = {
    if (!this.boundary.contains(point) || !insertPoint(point)) {

      //            insertPoint(point)

      throw new Exception("Point insert failed: %s in QuadTree: %s".format(point, this))
    }

    true
  }

  private def insertPoint(point: Point): Boolean = {

    var qTree = this

    //        while (qTree.boundary.contains(point))
    while (true) {

      qTree.totalPoints += 1

      if (qTree.points.size < capacity) {

        qTree.points += point
        return true
      }
      else
      // switch to proper quadrant?
        qTree = if (point.x <= qTree.boundary.center.x)
          if (point.y >= qTree.boundary.center.y) {

            if (qTree.topLeft == null)
              qTree.topLeft = new QuadTree(qTree.boundary.topLeftQuadrant /*, qTree*/)

            qTree.topLeft
          }
          else {

            if (qTree.bottomLeft == null)
              qTree.bottomLeft = new QuadTree(qTree.boundary.bottomLeftQuadrant /*, qTree*/)

            qTree.bottomLeft
          }
        else if (point.y >= qTree.boundary.center.y) {

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

  def isLeaf: Boolean = topLeft == null && topRight == null && bottomLeft == null && bottomRight == null

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
