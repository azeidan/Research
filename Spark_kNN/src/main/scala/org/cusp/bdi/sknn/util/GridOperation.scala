package org.cusp.bdi.sknn.util

class GridOperation(mbr: (Double, Double, Double, Double), totalRowCount: Long, k: Int) extends Serializable {

  private val pointPerBox = (totalRowCount.toDouble / k).toLong + 1

  private val boxWH = math.max(math.ceil((mbr._4 - mbr._2) / pointPerBox), math.ceil((mbr._3 - mbr._1) / pointPerBox))

  def getBoxWH = boxWH

  def computeBoxXY(xy: (Double, Double)): (Double, Double) =
    computeBoxXY(xy._1, xy._2)

  def computeBoxXY(x: Double, y: Double): (Double, Double) =
    (math.floor(x / boxWH), math.floor(y / boxWH))
}