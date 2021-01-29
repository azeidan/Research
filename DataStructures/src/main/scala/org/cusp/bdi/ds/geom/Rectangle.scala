package org.cusp.bdi.ds.geom

import org.cusp.bdi.util.Helper

case class Rectangle(center: Geom2D, halfXY: Geom2D) extends Serializable {

  def this(other: Rectangle) =
    this(new Geom2D(other.center), new Geom2D(other.halfXY))

  def contains(x: Double, y: Double) =
    Helper.absDiff(x, this.center.x) <= this.halfXY.x && Helper.absDiff(y, this.center.y) <= this.halfXY.y

  def contains(point: Geom2D): Boolean =
    contains(point.x, point.y)

  def contains(xy: (Double, Double)): Boolean =
    contains(xy._1, xy._2)

  def mergeWith(other: Rectangle): Unit =
    if (other != null) {

      val minX = math.min(this.left, other.left)
      val minY = math.min(this.bottom, other.bottom)
      val maxX = math.max(this.right, other.right)
      val maxY = math.max(this.top, other.top)

      this.halfXY.x = (maxX - minX) / 2
      this.halfXY.y = (maxY - minY) / 2

      this.center.x = minX + this.halfXY.x
      this.center.y = minY + this.halfXY.y
    }

  def left: Double =
    center.x - halfXY.x

  def bottom: Double =
    center.y - halfXY.y

  def right: Double =
    center.x + halfXY.x

  def top: Double =
    center.y + halfXY.y

  def intersects(other: Rectangle): Boolean = {

    lazy val otherLeft = other.left
    lazy val otherBottom = other.bottom
    lazy val otherRight = other.right
    lazy val otherTop = other.top

    intersects(otherLeft, otherBottom, otherRight, otherTop)
  }

  def intersects(otherLeft: => Double, otherBottom: => Double, otherRight: => Double, otherTop: => Double): Boolean = {

    lazy val thisLeft = this.left
    lazy val thisBottom = this.bottom
    lazy val thisRight = this.right
    lazy val thisTop = this.top

    !(thisLeft > otherRight ||
      thisRight < otherLeft ||
      thisTop < otherBottom ||
      thisBottom > otherTop)
  }

  def topLeftQuadrant: Rectangle =
    Rectangle(new Geom2D(center.x - halfXY.x / 2, center.y + halfXY.y / 2), quarterDim)

  def topRightQuadrant: Rectangle =
    Rectangle(new Geom2D(center.x + halfXY.x / 2, center.y + halfXY.y / 2), quarterDim)

  def bottomLeftQuadrant: Rectangle =
    Rectangle(new Geom2D(center.x - halfXY.x / 2, center.y - halfXY.y / 2), quarterDim)

  def bottomRightQuadrant: Rectangle =
    Rectangle(new Geom2D(center.x + halfXY.x / 2, center.y - halfXY.y / 2), quarterDim)

  def quarterDim =
    new Geom2D(halfXY.x / 2, halfXY.y / 2)

  override def toString: String =
    "%s\t%s".format(center, halfXY)
}