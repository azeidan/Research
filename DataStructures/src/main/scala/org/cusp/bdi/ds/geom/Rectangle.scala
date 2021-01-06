package org.cusp.bdi.ds.geom

case class Rectangle(center: Geom2D, halfXY: Geom2D) extends Serializable {

  def this(other: Rectangle) =
    this(new Geom2D(other.center), new Geom2D(other.halfXY))

  def halveBy(splitAlongX: Boolean): (Rectangle, Rectangle) = {

    val (newHalfX, newHalfY) = if (splitAlongX) (this.halfXY.x / 2, this.halfXY.y) else (this.halfXY.x, this.halfXY.y / 2)

    (Rectangle(new Geom2D(this.left + newHalfX, this.bottom + newHalfY), new Geom2D(newHalfX, newHalfY)),
      Rectangle(new Geom2D(this.right - newHalfX, this.top - newHalfY), new Geom2D(newHalfX, newHalfY)))
  }

  def contains(other: Rectangle): Boolean =
    !(this.left > other.left || this.right < other.right ||
      this.bottom > other.bottom || this.top < other.top)

  def contains(point: Geom2D): Boolean =
    !(point.x < left || point.x > right || point.y < bottom || point.y > top)

  def contains(xy: (Double, Double)): Boolean =
    !(xy._1 < left || xy._1 > right || xy._2 < bottom || xy._2 > top)

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

  def top: Double =
    center.y + halfXY.y

  def bottom: Double =
    center.y - halfXY.y

  def right: Double =
    center.x + halfXY.x

  def left: Double =
    center.x - halfXY.x

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
    Rectangle(new Geom2D(center.x - halfXY.x / 2, center.y + halfXY.y / 2), quarterDim)

  def topRightQuadrant: Rectangle =
    Rectangle(new Geom2D(center.x + halfXY.x / 2, center.y + halfXY.y / 2), quarterDim)

  def bottomLeftQuadrant: Rectangle =
    Rectangle(new Geom2D(center.x - halfXY.x / 2, center.y - halfXY.y / 2), quarterDim)

  def bottomRightQuadrant: Rectangle =
    Rectangle(new Geom2D(center.x + halfXY.x / 2, center.y - halfXY.y / 2), quarterDim)

  def quarterDim =
    new Geom2D(halfXY.x / 2, halfXY.y / 2)

  def mbr: String =
    "%.10f,%.10f,%.10f,%.10f".format(left, bottom, right, top)

  override def toString: String =
    "%s\t%s".format(center, halfXY)

  //  override def write(kryo: Kryo, output: Output) {
  //
  //    kryo.writeClassAndObject(output, center)
  //    kryo.writeClassAndObject(output, halfXY)
  //  }
  //
  //  override def read(kryo: Kryo, input: Input) {
  //
  //    center = kryo.readClassAndObject(input) match {
  //      case geom2D: Geom2D => geom2D
  //    }
  //    halfXY = kryo.readClassAndObject(input) match {
  //      case geom2D: Geom2D => geom2D
  //    }
  //  }
}