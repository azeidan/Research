package org.cusp.bdi.sknn.util

final class GridOperation(mbrDS1Left: Double, mbrDS1Bottom: Double, mbrDS1Right: Double, mbrDS1Top: Double, totalRowCount: Long, k: Int) extends Serializable {

  private val pointPerSquare: Int = {

    val tmp = totalRowCount / k

    if (tmp >= Int.MaxValue) Int.MaxValue else tmp.toInt + 1 // ceil(...
  }

  val squareDim: Int = {

    val tmp = math.max(mbrDS1Right - mbrDS1Left, mbrDS1Top - mbrDS1Bottom) / pointPerSquare

    if (tmp >= Int.MaxValue) Int.MaxValue else tmp.toInt + 1 // ceil(...
  }

  def computeSquareXY(x: Double, y: Double): (Double, Double) =
    ((x / squareDim).floor, (y / squareDim).floor)
}