package org.cusp.bdi.sknn.util

case class GridOperation(mbr: (Double, Double, Double, Double), totalRowCount: Long, k: Int) extends Serializable {

  private val pointPerBox = math.ceil(totalRowCount.toDouble / k).toLong

  private val boxWH = math.max(math.ceil((mbr._4 - mbr._2) / pointPerBox), math.ceil((mbr._3 - mbr._1) / pointPerBox))

  private val shiftErrorRange = 2 * math.sqrt((boxWH - 1) / boxWH)

  def getBoxWH = boxWH

  def getShiftErrorRange = shiftErrorRange

  def computeBoxXY(xy: (Double, Double)): (Long, Long) =
    computeBoxXY(xy._1, xy._2)

  def computeBoxXY(x: Double, y: Double): (Long, Long) = {

    val scaled = scaleXY(x, y)

    (math.floor(scaled._1).toLong, math.floor(scaled._2).toLong)
  }

  def scaleXY(x: Double, y: Double) =
    (x / boxWH, y / boxWH)

  def scaleXY(xy: (Double, Double)): (Double, Double) =
    scaleXY(xy._1, xy._2)

  //    (math.round(x / boxWH), math.round(y / boxWH))
}