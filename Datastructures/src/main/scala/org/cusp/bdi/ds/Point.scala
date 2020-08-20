package org.cusp.bdi.ds

case class Point() extends PointBase {

  var userData: Any = _

  def this(x: Double, y: Double) = {

    this()

    this.x = x
    this.y = y
  }

  def this(other: Point) =
    this(other.x, other.y)

  def this(x: Double, y: Double, userData: Any) = {

    this(x, y)

    this.userData = userData
  }

  def this(xy: (Double, Double), userData: Any) =
    this(xy._1, xy._2, userData)

  def this(xy: (Double, Double)) =
    this(xy._1, xy._2)

  override def equals(other: Any): Boolean = other match {
    case mpi: Point => this.compareTo(mpi) == 0
    case _ => false
  }

  def compareTo(other: Point) =
    if (userData == null || other.userData == null)
      super.compareTo(other)
    else
      userData.toString.compareTo(other.userData.toString)

  override def toString: String =
    "(%s,%s)".format(super.toString, if (userData == null) "" else "," + userData.toString)
}
