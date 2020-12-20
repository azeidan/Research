package org.cusp.bdi.ds.kdt

import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds.bt.{AVLNode, AVLTree}
import org.cusp.bdi.ds.geom.{Point, Rectangle}
import org.cusp.bdi.ds.kdt.Histogram.{TypeAVL, TypeAVL_Data}
import org.cusp.bdi.ds.kdt.KdtNode.SPLIT_VAL_NONE
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Histogram {

  type TypeAVL_Data = ListBuffer[(Int, Point)]
  type TypeAVL = AVLTree[TypeAVL_Data]

  def apply(iterPoints: Iterator[Point], hgGroupWidth: Int, lowerBounds: (Double, Double)): Histogram = {

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

    new Histogram(avlHistogram, setCoordY.size, pointCount)
  }
}

case class Histogram(avlTree: TypeAVL, otherIndexCount: Int, pointCount: Int) {

  def extractPointInfo(): (ListBuffer[Point], Rectangle) = {

    val lstPoints = ListBuffer[Point]()

    var minX = Double.MaxValue
    var minY = Double.MaxValue
    var maxX = Double.MinValue
    var maxY = Double.MinValue

    avlTree
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

  def partition(splitX: Boolean): (Double, Histogram, Histogram) = {

    val fAddToHG = (idxPoint: (Int, Point), avlTree: TypeAVL, currNodeVal: Int) => {

      val avlNode = avlTree.getOrElseInsert(idxPoint._1)

      if (avlNode.data == null)
        avlNode.data = new TypeAVL_Data()

      avlNode.data += ((currNodeVal, idxPoint._2))
    }
    val fExtractCoord = (point: Point) => if (splitX) point.x else point.y
    val pointCountHalf = pointCount / 2
    var pointCountPart1 = 0
    val stackNode = mutable.Stack[AVLNode[TypeAVL_Data]]()
    var currNode = avlTree.rootNode
    var lastNodePart1: Point = null
    val avlHistogramPart1 = new TypeAVL()
    val avlHistogramPart2 = new TypeAVL()
    var indexCountPart1 = 0 // counts the number of indexes subsumed into the new AVL tree. represents the number of indexes redirected to the new AVL
    var indexCountPart2 = 0
    //    var collectPart1 = true

    // inorder traversal
    while (currNode != null || stackNode.nonEmpty) {

      // left-most node
      while (currNode != null) {

        stackNode.push(currNode)
        currNode = currNode.left
      }

      currNode = stackNode.pop

      if (pointCountPart1 >= pointCountHalf) {

        indexCountPart2 += 1

        currNode.data.foreach(fAddToHG(_, avlHistogramPart2, currNode.nodeVal))
      }
      else {

        indexCountPart1 += 1

        val iterData = (
          if (pointCountPart1 + currNode.data.length >= pointCountHalf)
            currNode.data.sortBy(idxPoint => fExtractCoord(idxPoint._2))
          else
            currNode.data
          ).iterator

        while (iterData.hasNext) {

          val idxPoint = iterData.next

          if (pointCountPart1 < pointCountHalf) {

            pointCountPart1 += 1

            lastNodePart1 = idxPoint._2

            fAddToHG(idxPoint, avlHistogramPart1, currNode.nodeVal)
          }
          else {

            indexCountPart2 += 1

            if (fExtractCoord(lastNodePart1) == fExtractCoord(idxPoint._2))
              lastNodePart1 = null

            fAddToHG(idxPoint, avlHistogramPart2, currNode.nodeVal)

            iterData.foreach(fAddToHG(_, avlHistogramPart2, currNode.nodeVal))
          }
        }
      }

      currNode = currNode.right
    }

    val splitInfoPart1 = new Histogram(avlHistogramPart1, indexCountPart1, pointCountPart1)

    val splitInfoPart2 =
      if (pointCount - pointCountPart1 == 0)
        null
      else
        new Histogram(avlHistogramPart2, indexCountPart2, pointCount - pointCountPart1)

    (if (lastNodePart1 == null) SPLIT_VAL_NONE else fExtractCoord(lastNodePart1), splitInfoPart1, splitInfoPart2)

    //      if (pointCountPart1 == 0 || pointCountPart1 + currNode.data.length <= pointCountHalf /*&& (currNode != null || stackNode.nonEmpty)*/ ) {
    //
    //        splitNodeValue = currNode.nodeValue
    //
    //        pointCountPart1 += currNode.data.length
    //
    //        indexCountPart1 += 1
    //        addToAVL(avlHistogramPart1, currNode)
    //      }
    //      else {
    //
    //        indexCountPart2 += 1
    //        addToAVL(avlHistogramPart2, currNode)
    //
    //        collectPart1 = false
    //      }
    //      else {
    //
    //        indexCountPart2 += 1
    //        addToAVL(avlHistogramPart2, currNode)
    //      }
    //
    //      currNode = currNode.right
    //    }
    //
    //    val splitInfoPart1 = new Histogram(avlHistogramPart1, indexCountPart1, pointCountPart1)
    //
    //    val splitInfoPart2 =
    //      if (pointCount - pointCountPart1 == 0)
    //        null
    //      else
    //        new Histogram(avlHistogramPart2, indexCountPart2, pointCount - pointCountPart1)
    //
    //    (splitNodeValue, splitInfoPart1, splitInfoPart2)
  }
}