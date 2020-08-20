package org.cusp.bdi.ds

case class Circle(center: PointBase) extends Serializable {

  private var radius = 0.0

  def setRadius(radius: Double): Unit =
    this.radius = radius

  def getRadius: Double = radius

  def this(center: PointBase, radius: Double) = {

    this(center)
    this.radius = radius
  }

  def contains(box: Box): Boolean = {

    lazy val boxLeft = box.left
    lazy val boxRight = box.right
    lazy val boxTop = box.top
    lazy val boxBottom = box.bottom

    contains(boxLeft, boxBottom, boxRight, boxTop)
  }

  def contains(left: => Double, bottom: => Double, right: => Double, top: => Double): Boolean = {

    lazy val circleLeft = this.left
    lazy val circleRight = this.right
    lazy val circleTop = this.top
    lazy val circleBottom = this.bottom

    !(left <= circleLeft ||
      right >= circleRight ||
      top >= circleTop ||
      bottom <= circleBottom)
  }

  def left: Double = this.center.x - this.radius

  def right: Double = this.center.x + this.radius

  def top: Double = this.center.y + this.radius

  def bottom: Double = this.center.y - this.radius

  def intersects(mbr: (Double, Double, Double, Double)): Boolean =
    intersects(mbr._1, mbr._2, mbr._3, mbr._4)

  def intersects(otherLeft: => Double, otherBottom: => Double, otherRight: => Double, otherTop: => Double): Boolean = {

    lazy val circleLeft = this.center.x - this.radius
    lazy val circleRight = this.center.x + this.radius
    lazy val circleTop = this.center.y + this.radius
    lazy val circleBottom = this.center.y - this.radius

    !(otherLeft > circleRight ||
      otherRight < circleLeft ||
      otherBottom > circleTop ||
      otherTop < circleBottom)
  }

  def contains(mbr: (Double, Double, Double, Double)): Boolean =
    contains(mbr._1, mbr._2, mbr._3, mbr._4)

  def intersects(box: Box): Boolean = {

    lazy val boxLeft = box.left
    lazy val boxRight = box.right
    lazy val boxTop = box.top
    lazy val boxBottom = box.bottom

    intersects(boxLeft, boxBottom, boxRight, boxTop)
  }

  def expandRadius(expandBy: Double): Unit =
    radius += expandBy

  override def toString: String =
    "%f,%f,%f".format(center.x, center.y, radius)
}