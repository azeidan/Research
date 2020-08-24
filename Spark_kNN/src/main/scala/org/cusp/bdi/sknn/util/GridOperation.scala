package org.cusp.bdi.sknn.util

class GridOperation(mbr: (Double, Double, Double, Double), totalRowCount: Long, k: Int) extends Serializable {

  private val pointPerBox = (totalRowCount.toDouble / k).toLong + 1

  private val boxWH = math.max(math.ceil((mbr._4 - mbr._2) / pointPerBox), math.ceil((mbr._3 - mbr._1) / pointPerBox))

  private val errorRange = 2 * math.sqrt(2 * math.pow((boxWH - 1) / boxWH, 2))

  def getBoxWH = boxWH

  def getErrorRange = errorRange

  def computeBoxXY(xy: (Double, Double)): (Long, Long) =
    computeBoxXY(xy._1, xy._2)

  def computeBoxXY(x: Double, y: Double): (Long, Long) =
    (math.floor(x / boxWH).toLong, math.floor(y / boxWH).toLong)

  //  def scaleXY(xy: (Double, Double)): (Double, Double) =
  //    scaleXY(xy._1, xy._2)
  //
  //  def scaleXY(x: Double, y: Double) =
  //    (x / boxWH, y / boxWH)

  //    (math.round(x / boxWH), math.round(y / boxWH))
}