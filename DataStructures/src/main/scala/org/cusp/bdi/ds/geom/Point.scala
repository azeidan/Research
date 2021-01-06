package org.cusp.bdi.ds.geom

case class Point() extends Geom2D {

  var userData: Any = _

  def this(x: Double, y: Double) = {

    this()

    this.x = x
    this.y = y
  }

  def this(other: Point) = {

    this(other.x, other.y)
    this.userData = other.userData
  }

  def this(x: Double, y: Double, userData: Any) = {

    this(x, y)

    this.userData = userData
  }

  def this(xy: (Double, Double), userData: Any) =
    this(xy._1, xy._2, userData)

  def this(xy: (Double, Double)) =
    this(xy._1, xy._2)

  //  override def write(kryo: Kryo, output: Output): Unit = {
  //    super.write(kryo, output)
  //    kryo.writeClassAndObject(output, userData)
  //  }
  //
  //  override def read(kryo: Kryo, input: Input): Unit = {
  //    super.read(kryo, input)
  //    userData = kryo.readClassAndObject(input)
  //  }

  override def equals(other: Any): Boolean = other match {
    case pt: Point =>
      if (userData == null || pt.userData == null)
        this.x.equals(pt.x) && this.y.equals(pt.y)
      else
        userData.equals(pt.userData)
  }

  override def toString: String =
    "(%s,%s)".format(super.toString, if (userData == null) "" else userData.toString)
}
