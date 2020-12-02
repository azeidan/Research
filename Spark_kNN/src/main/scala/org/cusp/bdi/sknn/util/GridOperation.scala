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

  def computeSquareXY(xy: (Double, Double)): (Double, Double) =
    computeSquareXY(xy._1, xy._2)

  def computeSquareXY(x: Double, y: Double): (Double, Double) =
    ((x / squareDim).floor, (y / squareDim).floor)

  //  def reverseEstimateXY(x: Double, y: Double): (Double, Double) =
  //    (x * squareDim + squareDim, y * squareDim + squareDim)

  //  override def write(kryo: Kryo, output: Output): Unit = {
  //
  //    output.writeInt(pointPerSquare)
  //    output.writeInt(squareDim)
  //  }
  //
  //  override def read(kryo: Kryo, input: Input): Unit = {
  //
  //    pointPerSquare = input.readInt()
  //    squareDim = input.readInt()
  //  }
}