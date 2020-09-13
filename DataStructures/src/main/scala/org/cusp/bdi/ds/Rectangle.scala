package org.cusp.bdi.ds

case class Rectangle(pointCenter: PointBase, pointHalfXY: PointBase) extends Serializable {

  def this(other: Rectangle) =
    this(new PointBase(other.pointCenter), new PointBase(other.pointHalfXY))

  def contains(other: Rectangle): Boolean =
    !(this.left > other.left || this.right < other.right ||
      this.bottom > other.bottom || this.top < other.top)

  def contains(point: PointBase): Boolean =
    contains(point.x, point.y)

  def contains(x: Double, y: Double): Boolean =
    !(x < left || x > right || y < bottom || y > top)

  def top: Double = {
    pointCenter.y + pointHalfXY.y
  }

  def bottom: Double = {
    pointCenter.y - pointHalfXY.y
  }

  def right: Double = {
    pointCenter.x + pointHalfXY.x
  }

  def left: Double = {
    pointCenter.x - pointHalfXY.x
  }

  def contains(mbr: (Double, Double, Double, Double)): Boolean =
    left <= mbr._1 && bottom <= mbr._2 && right >= mbr._3 && top >= mbr._4

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
    Rectangle(new PointBase(pointCenter.x - pointHalfXY.x / 2, pointCenter.y + pointHalfXY.y / 2), quarterDim)

  def topRightQuadrant: Rectangle =
    Rectangle(new PointBase(pointCenter.x + pointHalfXY.x / 2, pointCenter.y + pointHalfXY.y / 2), quarterDim)

  def bottomLeftQuadrant: Rectangle =
    Rectangle(new PointBase(pointCenter.x - pointHalfXY.x / 2, pointCenter.y - pointHalfXY.y / 2), quarterDim)

  def bottomRightQuadrant: Rectangle =
    Rectangle(new PointBase(pointCenter.x + pointHalfXY.x / 2, pointCenter.y - pointHalfXY.y / 2), quarterDim)

  def quarterDim =
    new PointBase(pointHalfXY.x / 2, pointHalfXY.y / 2)

  def mbr: String =
    "%f,%f,%f,%f".format(left, bottom, right, top)

  override def toString: String =
    "%s\t%s".format(pointCenter, pointHalfXY)
}