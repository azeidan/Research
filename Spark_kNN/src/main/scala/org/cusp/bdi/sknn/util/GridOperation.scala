package org.cusp.bdi.sknn.util

class GridOperation(mbr: (Double, Double, Double, Double), totalRowCount: Long, k: Int) extends Serializable {

    private val pointPerBox = math.ceil(totalRowCount.toDouble / k).toLong

    private val boxWH = math.max(math.ceil((mbr._4 - mbr._2) / pointPerBox), math.ceil((mbr._3 - mbr._1) / pointPerBox))

    def computeBoxXY(x: Double, y: Double) =
        (math.floor(x / boxWH).toLong, math.floor(y / boxWH).toLong)
}