package org.cusp.bdi.sknn.util

case class GridOperation(mbr: (Double, Double, Double, Double), totalRowCount: Long, k: Int) extends Serializable {

  private val pointPerBox = (totalRowCount.toDouble / k).toLong + 1

  private val boxWH = math.max(((mbr._4 - mbr._2) / pointPerBox).toLong + 1, ((mbr._3 - mbr._1) / pointPerBox).toLong + 1)

  private val errorRange = 2 * math.sqrt(2 * (boxWH - 1.0) / boxWH)

  def getErrorRange = errorRange

  def getBoxWH = boxWH

  def computeBoxXY(x: Double, y: Double): (Long, Long) =
    ((x / boxWH).toLong, (y / boxWH).toLong)
}