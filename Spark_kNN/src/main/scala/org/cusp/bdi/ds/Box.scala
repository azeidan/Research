package org.cusp.bdi.ds

case class Box(center: PointBase, halfDimension: PointBase) extends Serializable {

  def this(other: Box) =
    this(new PointBase(other.center), new PointBase(other.halfDimension))

  def contains(other: Box): Boolean =
    !(this.left > other.left || this.right < other.right ||
      this.bottom > other.bottom || this.top < other.top)

  def contains(point: PointBase): Boolean =
    contains(point.x, point.y)

  def contains(x: Double, y: Double): Boolean =
    !(x < left || x > right || y < bottom || y > top)

  def top: Double = {
    center.y + halfDimension.y
  }

  def bottom: Double = {
    center.y - halfDimension.y
  }

  def right: Double = {
    center.x + halfDimension.x
  }

  def left: Double = {
    center.x - halfDimension.x
  }

  def contains(mbr: (Double, Double, Double, Double)): Boolean =
    left <= mbr._1 && bottom <= mbr._2 && right >= mbr._3 && top >= mbr._4

  def intersects(other: Box): Boolean = {

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

  def topLeftQuadrant: Box =
    Box(new PointBase(center.x - halfDimension.x / 2, center.y + halfDimension.y / 2), quarterDim)

  def topRightQuadrant: Box =
    Box(new PointBase(center.x + halfDimension.x / 2, center.y + halfDimension.y / 2), quarterDim)

  def bottomLeftQuadrant: Box =
    Box(new PointBase(center.x - halfDimension.x / 2, center.y - halfDimension.y / 2), quarterDim)

  def quarterDim =
    new PointBase(halfDimension.x / 2, halfDimension.y / 2)

  def bottomRightQuadrant: Box =
    Box(new PointBase(center.x + halfDimension.x / 2, center.y - halfDimension.y / 2), quarterDim)

  def mbr: String =
    "%f,%f,%f,%f".format(left, bottom, right, top)

  override def toString: String =
    "%s\t%s".format(center, halfDimension)
}