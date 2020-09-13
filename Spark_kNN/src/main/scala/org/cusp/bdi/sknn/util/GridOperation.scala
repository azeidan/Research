package org.cusp.bdi.sknn.util

final class GridOperation(mbrDS1Left: Double, mbrDS1Bottom: Double, mbrDS1Right: Double, mbrDS1Top: Double, totalRowCount: Long, k: Int) extends Serializable {

  private val pointPerSquare = (totalRowCount.toDouble / k).toLong + 1

  val squareLen: Double = math.max(math.ceil((mbrDS1Right - mbrDS1Left) / pointPerSquare), math.ceil((mbrDS1Top - mbrDS1Bottom) / pointPerSquare))

  def computeSquareXY(x: Double, y: Double): (Double, Double) =
    (math.floor(x / squareLen), math.floor(y / squareLen))
}