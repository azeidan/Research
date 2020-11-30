package org.cusp.bdi.sknn.util

final class GridOperation extends Serializable {

  private var pointPerSquare: Int = -1

  var squareDim: Int = -1

  def this(datasetMBR: (Double, Double, Double, Double), totalRowCount: Long, k: Int) = {

    this()

    var tmp = totalRowCount / k
    pointPerSquare = if (tmp >= Int.MaxValue) Int.MaxValue else tmp.toInt + 1 // ceil(...

    tmp = (math.max(datasetMBR._3 - datasetMBR._1, datasetMBR._4 - datasetMBR._2) / pointPerSquare).toInt
    squareDim = if (tmp >= Int.MaxValue) Int.MaxValue else tmp.toInt + 1 // ceil(...
  }

  def computeSquareXY(x: Double, y: Double): (Double, Double) =
    ((x / squareDim).floor, (y / squareDim).floor)
}