package org.cusp.bdi.ds

case class Point() extends PointBase {

  var userData: Any = _

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

  def this(xy: (Double, Double)) =
    this(xy._1, xy._2)

  override def clone() =
    new Point(x, y)

  override def equals(other: Any): Boolean = other match {
    case pt: Point =>
      if (userData == null || pt.userData == null)
        super.equals(other)
      else
        userData.equals(pt.userData)
    case _ =>
      false
  }

  //  override def compareTo(other: PointBase): Int = {
  //
  //    other match {
  //      case point: Point =>
  //        if (userData == null || point.userData == null)
  //          super.compareTo(other)
  //        else
  //          userData.compareTo(point.userData)
  //    }
  //  }

  override def toString: String =
    "(%.22f,%.22f,%s)".format(x, y, if (userData == null) "" else  userData.toString)
}
