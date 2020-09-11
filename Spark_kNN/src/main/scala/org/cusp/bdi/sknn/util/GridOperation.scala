package org.cusp.bdi.sknn.util

final class GridOperation(mbrDS1Left: Double, mbrDS1Bottom: Double, mbrDS1Right: Double, mbrDS1Top: Double, totalRowCount: Long, k: Int) extends Serializable {

  private val pointPerBox = (totalRowCount.toDouble / k).toLong + 1

  val boxWH: Double = math.max(math.ceil((mbrDS1Right - mbrDS1Left) / pointPerBox), math.ceil((mbrDS1Top - mbrDS1Bottom) / pointPerBox))

  def computeBoxXY(x: Double, y: Double): (Double, Double) =
    (math.floor(x / boxWH), math.floor(y / boxWH))
}