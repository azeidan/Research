package org.cusp.bdi.ds

class PointBase extends Serializable with Comparable[PointBase] {

  var x = 0.0
  var y = 0.0

  def this(x: Double, y: Double) = {

    this()

    this.x = x

    this.y = y
  }

  def this(other: PointBase) =
    this(other.x, other.y)

  def this(xy: (Double, Double)) =
    this(xy._1, xy._2)

  override def equals(other: Any): Boolean = other match {
    case pb: PointBase => this.compareTo(pb) == 0
    case _ => false
  }

  override def compareTo(other: PointBase): Int = {

    val res = this.x.compareTo(other.x)

    if (res == 0)
      this.y.compareTo(other.y)
    else
      res
  }

  override def toString: String =
    "%.22f,%.22f".format(x, y)
}
