package com.insightfullogic.quad_trees

case class Point() extends Serializable with Comparable[Point] {

    var x = 0.0
    var y = 0.0

    var userData: Any = null

    def this(x: Double, y: Double) = {

        this()

        this.x = x
        this.y = y
    }

    def this(point: Point) =
        this(point.x, point.y)

    def this(x: Double, y: Double, userData: Any) = {

        this(x, y)

        this.userData = userData
    }

    def this(xy: (Double, Double), userData: Any) =
        this(xy._1, xy._2, userData)

    override def clone() =
        new Point(x, y)

    override def compareTo(other: Point) = {

        if (userData == null || other.userData == null) {

            var res = this.x.compareTo(other.x)

            if (res == 0)
                this.y.compareTo(other.y)
            else
                res
        }
        else
            userData.toString.compareTo(other.userData.toString)
    }

    override def equals(other: Any) = other match {
        case mpi: Point => this.compareTo(mpi) == 0
        case _ => false
    }

    override def toString() =
        "(%.22f,%.22f%s)".format(x, y, if (userData == null) "" else "," + userData.toString())
}
