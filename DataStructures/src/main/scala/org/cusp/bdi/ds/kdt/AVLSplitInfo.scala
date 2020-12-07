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

      var idxX = pt.x - lowerBounds._1
      var idxY = pt.y - lowerBounds._2

      if (hgGroupWidth > 1) {

        idxX = idxX / hgGroupWidth
        idxY = idxY / hgGroupWidth
      }

      setCoordY += idxY.toInt

      val avlNode = avlHistogram.getOrElseInsert(idxX.toInt)

      if (avlNode.data == null)
        avlNode.data = new TypeAVL_Data()

      avlNode.data += ((idxY.toInt, pt))
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

    avlHistogram
      .allNodes
      .foreach(_.data.foreach(row => {

        lstPoints += row._2

        if (row._2.x < minX) minX = row._2.x
        if (row._2.x > maxX) maxX = row._2.x

        if (row._2.y < minY) minY = row._2.y
        if (row._2.y > maxY) maxY = row._2.y
      }))

    (lstPoints, buildRectBounds((minX, minY), (maxX, maxY)))
  }

  def canPartition(nodeCapacity: Int): Boolean = {

    if (avlHistogram.rootNode.treeHeight > 1 || otherIndexCount > 1) {

      var count = 0

      avlHistogram
        .allNodes
        .foreach(node => {

          count += node.data.length

          if (count >= nodeCapacity)
            return true
        })
    }

    false
  }

  def partition: (Double, AVLSplitInfo, AVLSplitInfo) = {

    def addToAVL(avlTree: TypeAVL, currNode: AVLNode[TypeAVL_Data]): Unit =
      currNode
        .data
        .foreach(row => {

          val avlNode = avlTree.getOrElseInsert(row._1)

          if (avlNode.data == null)
            avlNode.data = new TypeAVL_Data()

          avlNode.data += ((currNode.nodeValue, row._2))
        })

    // find the split node
    val pointCountHalf = pointCount / 2
    var pointCountPart1 = 0
    val stackNode = mutable.Stack[AVLNode[TypeAVL_Data]]()
    var currNode = avlHistogram.rootNode
    var splitNodeValue = currNode.nodeValue
    val avlHistogramPart1 = new TypeAVL()
    val avlHistogramPart2 = new TypeAVL()
    var avlIndexCountPart1 = 0 // counts the number of indexes subsumed into the new AVL tree. represents the number of indexes redirected to the new AVL
    var avlIndexCountPart2 = 0
    var collectPart1 = true

    // inorder traversal
    while (currNode != null || stackNode.nonEmpty) {

      // left-most node
      while (currNode != null) {

        stackNode.push(currNode)
        currNode = currNode.left
      }

      currNode = stackNode.pop

      if (collectPart1)
        if (pointCountPart1 == 0 || pointCountPart1 + currNode.data.length <= pointCountHalf /*&& (currNode != null || stackNode.nonEmpty)*/ ) {

          splitNodeValue = currNode.nodeValue

          pointCountPart1 += currNode.data.length

          avlIndexCountPart1 += 1
          addToAVL(avlHistogramPart1, currNode)
        }
        else {

          avlIndexCountPart2 += 1
          addToAVL(avlHistogramPart2, currNode)

          collectPart1 = false
        }
      else {

        avlIndexCountPart2 += 1
        addToAVL(avlHistogramPart2, currNode)
      }

      currNode = currNode.right
    }

    val avlSplitInfoPart1 = new AVLSplitInfo(avlHistogramPart1, avlIndexCountPart1, pointCountPart1)

    val avlSplitInfoPart2 =
      if (pointCount - pointCountPart1 == 0)
        null
      else
        new AVLSplitInfo(avlHistogramPart2, avlIndexCountPart2, pointCount - pointCountPart1)

    (splitNodeValue, avlSplitInfoPart1, avlSplitInfoPart2)
  }
}