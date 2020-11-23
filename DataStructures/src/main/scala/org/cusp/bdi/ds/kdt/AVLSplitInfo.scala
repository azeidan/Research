package org.cusp.bdi.ds.kdt

import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds.bt.{AVLNode, AVLTree}
import org.cusp.bdi.ds.geom.{Point, Rectangle}
import org.cusp.bdi.ds.kdt.AVLSplitInfo.{TypeAVL, TypeAVL_Data}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object AVLSplitInfo {

  type TypeAVL_Data = ListBuffer[(Int, Point)]
  type TypeAVL = AVLTree[TypeAVL_Data]

  def apply(iterPoints: Iterator[Point], hgGroupWidth: Int, lowerBounds: (Double, Double)): AVLSplitInfo = {

    val setCoordY = mutable.Set[Int]()
    var pointCount = 0
    val avlHistogram = new TypeAVL()

    iterPoints.foreach(pt => {

      pointCount += 1

      val idxX = ((pt.x - lowerBounds._1) / hgGroupWidth).toInt
      val idxY = ((pt.y - lowerBounds._2) / hgGroupWidth).toInt

      setCoordY += idxY

      val avlNode = avlHistogram.getOrElseInsert(idxX)

      if (avlNode.data == null)
        avlNode.data = new TypeAVL_Data()

      avlNode.data += ((idxY, pt))
    })

    new AVLSplitInfo(avlHistogram, setCoordY.size, pointCount)
  }
}

case class AVLSplitInfo(avlHistogram: TypeAVL, otherIndexCount: Int, pointCount: Int) {

  def extractPointInfo(): (ListBuffer[Point], Rectangle) = {

    val lstPoints = ListBuffer[Point]()

    var minX = Double.MaxValue
    var minY = Double.MaxValue
    var maxX = Double.MinValue
    var maxY = Double.MinValue

    avlHistogram.allNodes
      .foreach(_.data.foreach(row => {

        lstPoints += row._2

        if (row._2.x < minX) minX = row._2.x
        if (row._2.x > maxX) maxX = row._2.x

        if (row._2.y < minY) minY = row._2.y
        if (row._2.y > maxY) maxY = row._2.y
      }))

    (lstPoints, buildRectBounds((minX, minY), (maxX, maxY)))
  }

  def canPartition: Boolean =
    avlHistogram.rootNode.treeHeight > 1 || otherIndexCount > 1

  def partition(): (Double, AVLSplitInfo, AVLSplitInfo) = {

    def addToAVL(avlTree: TypeAVL, currNode: AVLNode[TypeAVL_Data]): Unit =
      currNode
        .data
        .foreach(row => {

          val avlNodeNew = avlTree.getOrElseInsert(row._1)

          if (avlNodeNew.data == null)
            avlNodeNew.data = new TypeAVL_Data()

          avlNodeNew.data += ((currNode.keyValue, row._2))
        })

    // find the split node
    val pointCountHalf = pointCount / 2
    var pointCountPart1 = 0
    val stackNode = mutable.Stack[AVLNode[TypeAVL_Data]]()
    var currNode = avlHistogram.rootNode
    var splitKeyValue = currNode.keyValue
    val avlHistogramPart1 = new TypeAVL()
    val avlHistogramPart2 = new TypeAVL()
    var otherIndexCountPart1 = 0
    var otherIndexCountPart2 = 0
    var collectPart1 = true

    // find split value
    while (currNode != null || stackNode.nonEmpty) {

      while (currNode != null) {

        stackNode.push(currNode)
        currNode = currNode.left
      }

      currNode = stackNode.pop

      if (collectPart1)
        if (pointCountPart1 == 0 || (pointCountPart1 + currNode.data.size <= pointCountHalf && (currNode != null || stackNode.nonEmpty))) {

          splitKeyValue = currNode.keyValue

          pointCountPart1 += currNode.data.size

          otherIndexCountPart1 += 1

          addToAVL(avlHistogramPart1, currNode)
        }
        else {

          otherIndexCountPart2 += 1

          addToAVL(avlHistogramPart2, currNode)

          collectPart1 = false
        }
      else {

        otherIndexCountPart2 += 1
        addToAVL(avlHistogramPart2, currNode)
      }

      currNode = currNode.right
    }

    val avlSplitInfoPart1 = new AVLSplitInfo(avlHistogramPart1, otherIndexCountPart1, pointCountPart1)

    val avlSplitInfoPart2 = if (pointCount - pointCountPart1 == 0)
      null
    else
      new AVLSplitInfo(avlHistogramPart2, otherIndexCountPart2, pointCount - pointCountPart1)

    (splitKeyValue, avlSplitInfoPart1, avlSplitInfoPart2)
  }
}