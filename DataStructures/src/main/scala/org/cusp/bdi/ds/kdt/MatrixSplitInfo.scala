package org.cusp.bdi.ds.kdt

import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds.geom.{Point, Rectangle}
import org.cusp.bdi.ds.kdt.KdTree.TypeMatrixLst

import scala.collection.mutable.ListBuffer

// reintroduce minXSplitIdx, maxXSplit ...
case class MatrixSplitInfo(matrixHistogram: TypeMatrixLst, pointCount: Int, arrMatrixIdxX: Array[Int], startIdx_arrMatrixIdxX: Int, endIdx_arrMatrixIdxX: Int, arrMatrixIdxY: Array[Int], startIdx_arrMatrixIdxY: Int, endIdx_arrMatrixIdxY: Int) {

  def extractPointInfo(): (ListBuffer[Point], Rectangle) = {

    val lstPoints = ListBuffer[Point]()
    var minX = Double.MaxValue
    var minY = Double.MaxValue
    var maxX = Double.MinValue
    var maxY = Double.MinValue

    (startIdx_arrMatrixIdxX to endIdx_arrMatrixIdxX)
      .map(idxX =>
        (startIdx_arrMatrixIdxY to endIdx_arrMatrixIdxY)
          .filter(idxY => matrixHistogram(arrMatrixIdxX(idxX))(arrMatrixIdxY(idxY)) != null)
          .map(idxY => matrixHistogram(arrMatrixIdxX(idxX))(arrMatrixIdxY(idxY)).foreach(pt => {

            lstPoints += pt

            if (pt.x < minX) minX = pt.x
            if (pt.x > maxX) maxX = pt.x

            if (pt.y < minY) minY = pt.y
            if (pt.y > maxY) maxY = pt.y
          })
          ))

    (lstPoints, buildRectBounds((minX, minY), (maxX, maxY)))
  }

  def canPartition(splitX: Boolean): Boolean =
    MatrixSplitOperation(splitX, this).canPartition()

  def partition(splitX: Boolean): (MatrixSplitInfo, MatrixSplitInfo) =
    MatrixSplitOperation(splitX, this).partition()

  def getSplitIdx(splitX: Boolean): Int =
    MatrixSplitOperation(splitX, this).getSplitIdx
}

object MatrixSplitOperation {

  def apply(splitX: Boolean, matrixSplitInfo: MatrixSplitInfo): MatrixSplitOperation =
    if (splitX)
      new MatrixSplitOperationX(matrixSplitInfo)
    else
      new MatrixSplitOperationY(matrixSplitInfo)
}

abstract class MatrixSplitOperation() {

  def canPartition(): Boolean

  def extractLstPoint(idxSplit_arrMatrixIdx: Int): IndexedSeq[ListBuffer[Point]]

  def fCoordExtract: Point => Double

  def partition(): (MatrixSplitInfo, MatrixSplitInfo)

  def getSplitIdx: Int
}

final class MatrixSplitOperationX(matrixSplitInfo: MatrixSplitInfo) extends MatrixSplitOperation() {

  override def fCoordExtract: Point => Double =
    (point: Point) => point.x

  override def getSplitIdx: Int =
    matrixSplitInfo.arrMatrixIdxX(matrixSplitInfo.endIdx_arrMatrixIdxX)

  override def canPartition(): Boolean = {

    var count = 0
    val rangeOuter = matrixSplitInfo.startIdx_arrMatrixIdxX to matrixSplitInfo.endIdx_arrMatrixIdxX

    if (rangeOuter.size > 1) {

      val iterX = rangeOuter.iterator

      while (iterX.hasNext && count < 2) {

        val idxSplit_arrMatrixIdxX = iterX.next

        //        if (matrixSplitInfo.matrixHistogram(matrixSplitInfo.arrMatrixIdxX(idxSplit_arrMatrixIdxX)) != null) {

        var found = false
        val iterY = (matrixSplitInfo.startIdx_arrMatrixIdxY to matrixSplitInfo.endIdx_arrMatrixIdxY).iterator

        while (iterY.hasNext && !found) {

          val idxSplit_arrMatrixIdxY = iterY.next

          if (matrixSplitInfo.matrixHistogram(matrixSplitInfo.arrMatrixIdxX(idxSplit_arrMatrixIdxX))(matrixSplitInfo.arrMatrixIdxY(idxSplit_arrMatrixIdxY)) != null) {

            found = true
            count += 1
          }
        }
        //        }
      }
    }

    count == 2
  }

  override def extractLstPoint(idxSplit_arrMatrixIdxX: Int): IndexedSeq[ListBuffer[Point]] =
    (matrixSplitInfo.startIdx_arrMatrixIdxY to matrixSplitInfo.endIdx_arrMatrixIdxY)
      .map(idxY => matrixSplitInfo.matrixHistogram(matrixSplitInfo.arrMatrixIdxX(idxSplit_arrMatrixIdxX))(matrixSplitInfo.arrMatrixIdxY(idxY)))
      .filter(_ != null)

  override def partition(): (MatrixSplitInfo, MatrixSplitInfo) = {

    var leftSidePointCount = 0
    val pointCountHalf = matrixSplitInfo.pointCount / 2
    val range = matrixSplitInfo.startIdx_arrMatrixIdxX to matrixSplitInfo.endIdx_arrMatrixIdxX
    var idxSplit_arrMatrixIdxX = -1

    var iterRange = range.iterator

    def process() {
      while (iterRange.hasNext && leftSidePointCount < pointCountHalf) {

        idxSplit_arrMatrixIdxX = iterRange.next()

        extractLstPoint(idxSplit_arrMatrixIdxX)
          .foreach(leftSidePointCount += _.size)
      }
    }

    process()

    if (!iterRange.hasNext) {

      leftSidePointCount = 0
      iterRange = range.take((range.size / 2.0).ceil.toInt).iterator
      process()
    }

    val msiLeftSide = MatrixSplitInfo(matrixSplitInfo.matrixHistogram, leftSidePointCount, matrixSplitInfo.arrMatrixIdxX, matrixSplitInfo.startIdx_arrMatrixIdxX, idxSplit_arrMatrixIdxX, matrixSplitInfo.arrMatrixIdxY, matrixSplitInfo.startIdx_arrMatrixIdxY, matrixSplitInfo.endIdx_arrMatrixIdxY)

    val rightSidePointCount = matrixSplitInfo.pointCount - leftSidePointCount

    val msiRightSide =
      if (rightSidePointCount == 0)
        null
      else
        MatrixSplitInfo(matrixSplitInfo.matrixHistogram, rightSidePointCount, matrixSplitInfo.arrMatrixIdxX, idxSplit_arrMatrixIdxX + 1, matrixSplitInfo.endIdx_arrMatrixIdxX, matrixSplitInfo.arrMatrixIdxY, matrixSplitInfo.startIdx_arrMatrixIdxY, matrixSplitInfo.endIdx_arrMatrixIdxY)

    (msiLeftSide, msiRightSide)
  }
}

final class MatrixSplitOperationY(matrixSplitInfo: MatrixSplitInfo) extends MatrixSplitOperation() {

  override def partition(): (MatrixSplitInfo, MatrixSplitInfo) = {

    var bottomSidePointCount = 0
    val pointCountHalf = matrixSplitInfo.pointCount / 2
    val range = matrixSplitInfo.startIdx_arrMatrixIdxY to matrixSplitInfo.endIdx_arrMatrixIdxY
    var idxSplit_arrMatrixIdxY = -1

    var iterRange = range.iterator

    def process() {

      while (iterRange.hasNext && bottomSidePointCount < pointCountHalf) {

        idxSplit_arrMatrixIdxY = iterRange.next()

        extractLstPoint(idxSplit_arrMatrixIdxY)
          .foreach(bottomSidePointCount += _.size)
      }
    }

    process()

    if (!iterRange.hasNext) {

      bottomSidePointCount = 0
      iterRange = range.take((range.size / 2.0).ceil.toInt).iterator
      process()
    }

    val topSidePointCount = matrixSplitInfo.pointCount - bottomSidePointCount

    val msiBottomSide = MatrixSplitInfo(matrixSplitInfo.matrixHistogram, bottomSidePointCount, matrixSplitInfo.arrMatrixIdxX, matrixSplitInfo.startIdx_arrMatrixIdxX, matrixSplitInfo.endIdx_arrMatrixIdxX, matrixSplitInfo.arrMatrixIdxY, matrixSplitInfo.startIdx_arrMatrixIdxY, idxSplit_arrMatrixIdxY)

    val msiTopSide =
      if (topSidePointCount == 0)
        null
      else
        MatrixSplitInfo(matrixSplitInfo.matrixHistogram, topSidePointCount, matrixSplitInfo.arrMatrixIdxX, matrixSplitInfo.startIdx_arrMatrixIdxX, matrixSplitInfo.endIdx_arrMatrixIdxX, matrixSplitInfo.arrMatrixIdxY, idxSplit_arrMatrixIdxY + 1, matrixSplitInfo.endIdx_arrMatrixIdxY)

    (msiBottomSide, msiTopSide)
  }

  override def fCoordExtract: Point => Double =
    (point: Point) => point.y

  override def getSplitIdx: Int =
    matrixSplitInfo.arrMatrixIdxY(matrixSplitInfo.endIdx_arrMatrixIdxY)

  override def canPartition(): Boolean = {

    var count = 0
    val rangeOuter = matrixSplitInfo.startIdx_arrMatrixIdxY to matrixSplitInfo.endIdx_arrMatrixIdxY

    if (rangeOuter.size > 1) {

      val iterY = rangeOuter.iterator

      while (iterY.hasNext && count < 2) {

        val idxSplit_arrMatrixIdxY = iterY.next

        var found = false
        val iterX = (matrixSplitInfo.startIdx_arrMatrixIdxX to matrixSplitInfo.endIdx_arrMatrixIdxX).iterator

        while (iterX.hasNext && !found) {

          val idxSplit_arrMatrixIdxX = iterX.next

          if ( /*matrixSplitInfo.matrixHistogram(matrixSplitInfo.arrMatrixIdxX(idxSplit_arrMatrixIdxX)) != null &&*/
            matrixSplitInfo.matrixHistogram(matrixSplitInfo.arrMatrixIdxX(idxSplit_arrMatrixIdxX))(matrixSplitInfo.arrMatrixIdxY(idxSplit_arrMatrixIdxY)) != null) {

            found = true
            count += 1
          }
        }
      }
    }

    count == 2
  }

  override def extractLstPoint(idxSplit_arrMatrixIdxY: Int): IndexedSeq[ListBuffer[Point]] =
    (matrixSplitInfo.startIdx_arrMatrixIdxX to matrixSplitInfo.endIdx_arrMatrixIdxX)
      .map(idxSplit_arrMatrixIdxX => matrixSplitInfo.matrixHistogram(matrixSplitInfo.arrMatrixIdxX(idxSplit_arrMatrixIdxX))(matrixSplitInfo.arrMatrixIdxY(idxSplit_arrMatrixIdxY)))
      .filter(_ != null)
}