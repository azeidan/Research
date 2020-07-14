package com.insightfullogic.quad_trees

case class Circle(center: Point) extends Serializable {

    private var radius = 0.0

    def setRadius(radius: Double) =
        this.radius = radius

    def getRadius() = radius

    def this(center: Point, radius: Double) = {

        this(center)
        this.radius = radius
    }

    def left = this.center.x - this.radius
    def right = this.center.x + this.radius
    def top = this.center.y + this.radius
    def bottom = this.center.y - this.radius

    def contains(box: Box): Boolean = {

        lazy val boxLeft = box.left
        lazy val boxRight = box.right
        lazy val boxTop = box.top
        lazy val boxBottom = box.bottom

        contains(boxLeft, boxBottom, boxRight, boxTop)
    }

    def intersects(mbr: (Double, Double, Double, Double)): Boolean =
        intersects(mbr._1, mbr._2, mbr._3, mbr._4)

    def contains(mbr: (Double, Double, Double, Double)): Boolean =
        contains(mbr._1, mbr._2, mbr._3, mbr._4)

    def contains(left: => Double, bottom: => Double, right: => Double, top: => Double) = {

        lazy val circleLeft = this.left
        lazy val circleRight = this.right
        lazy val circleTop = this.top
        lazy val circleBottom = this.bottom

        !(left <= circleLeft ||
            right >= circleRight ||
            top >= circleTop ||
            bottom <= circleBottom)
    }

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

    def intersects(other: Circle): Boolean = {

        lazy val otherLeft = this.center.x - this.radius
        lazy val otherRight = this.center.x + this.radius
        lazy val otherTop = this.center.y + this.radius
        lazy val otherBottom = this.center.y - this.radius

        intersects(otherLeft, otherBottom, otherRight, otherTop)
    }

    def intersects(box: Box): Boolean = {

        lazy val boxLeft = box.left
        lazy val boxRight = box.right
        lazy val boxTop = box.top
        lazy val boxBottom = box.bottom

        intersects(boxLeft, boxBottom, boxRight, boxTop)
    }

    def expandRadius(expandBy: Double) =
        radius += expandBy

    override def toString() =
        "%f,%f,%f".format(center.x, center.y, radius)
}