package org.cusp.bdi.sknn.util

import org.cusp.bdi.util.Helper

case class GridOperation(datasetMBR: (Double, Double, Double, Double)) extends Serializable {

  //  var squareDimX: Int = -1
  //  var squareDimY: Int = -1

  //  var shiftByX = -1
  //  var shiftByY = -1

  var squareDim: Int = -1

  def this(datasetMBR: (Double, Double, Double, Double), objCount: Long, k: Int) = {

    this(datasetMBR)

    val mbrMaxDim = Helper.max(datasetMBR._3 - datasetMBR._1, datasetMBR._4 - datasetMBR._2)
    //    val objPerSquare = Helper.log2(objCount) + Helper.log2(k)
    //    val squareCount = math.ceil(math.sqrt(objCount/log2(??))) // + Helper.log2(k)
    val squareCount = math.ceil(math.sqrt(objCount /*/ Helper.log2(k)*/))

    squareDim = math.ceil(mbrMaxDim / squareCount).toInt
  }

  def computeSquareXY(xy: (Double, Double)): (Double, Double) =
    computeSquareXY(xy._1, xy._2)

  def computeSquareXY(x: Double, y: Double): (Double, Double) =
    (((x - datasetMBR._1) / squareDim).floor, ((y - datasetMBR._2) / squareDim).floor)

  //    (((x - datasetMBR._1) / squareDimX).floor, ((y - datasetMBR._2) / squareDimY).floor)

  //  def reverseEstimateXY(x: Double, y: Double): (Double, Double) =
  //    (x * squareDimX + shiftByX, y * squareDimY + shiftByY)

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