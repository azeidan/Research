package org.cusp.bdi.ds.kdt

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.cusp.bdi.ds.SpatialIndex
import org.cusp.bdi.ds.SpatialIndex.{KnnLookupInfo, testAndAddPoint}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.KdTree.nodeCapacity
import org.cusp.bdi.ds.kdt.KdtNode.SPLIT_VAL_NONE
import org.cusp.bdi.ds.sortset.SortedLinkedList
import org.cusp.bdi.util.Helper

import scala.collection.mutable.ListBuffer
import scala.collection.{AbstractIterator, mutable}

object KdTree extends Serializable {

  val nodeCapacity = 4

  //  def findSearchRegionLocation(rectSearchRegion: Rectangle, nodeSplitVal: Double, splitX: Boolean): Char = {
  //
  //    lazy val limits = if (splitX)
  //      (rectSearchRegion.left, rectSearchRegion.right)
  //    else
  //      (rectSearchRegion.bottom, rectSearchRegion.top)
  //
  //    nodeSplitVal match {
  //      case SPLIT_VAL_NONE => 'B'
  //      case sVal if sVal < limits._1 => 'R' // region to the right (or below) of the splitKey
  //      case sVal if sVal > limits._2 => 'L' // region to the left (or above) of the splitKey
  //      case _ => 'B' // region contains the splitKey
  //    }
  //  }
}

class KdTree extends SpatialIndex {

  var rootNode: KdtNode = _

  def getTotalPoints: Int =
    rootNode.totalPoints

  override def dummyNode: AnyRef =
    new KdtBranchRootNode()

  override def estimateNodeCount(pointCount: Long): Int = {

    val height = Helper.log2(pointCount / nodeCapacity).toInt

    (1 until height).map(h => Math.pow(2, h - 1).toInt).sum
  }

  @throws(classOf[IllegalStateException])
  override def insert(rectBounds: Rectangle, iterPoints: Iterator[Point], histogramBarWidth: Int): Boolean = {

    if (rootNode != null) throw new IllegalStateException("KD Tree already built")
    if (rectBounds == null) throw new IllegalStateException("Rectangle bounds cannot be null")
    if (iterPoints.isEmpty) throw new IllegalStateException("Empty point iterator")
    if (histogramBarWidth < 1) throw new IllegalStateException("%s%d".format("Histogram bar width must be >= 1: Got: ", histogramBarWidth))

    val queueNodeInfo = mutable.Queue[(KdtBranchRootNode, Boolean, Histogram, Histogram)]()

    val lowerBounds = (rectBounds.left, rectBounds.bottom)

    def buildNode(nodeAVLSplitInfo: Histogram, splitX: Boolean): KdtNode = {

      if (nodeAVLSplitInfo.pointCount <= nodeCapacity)
        new KdtLeafNode(nodeAVLSplitInfo.extractPointInfo)
      else {

        val avlSplitInfoParts = nodeAVLSplitInfo.partition(splitX)

        val kdtBranchRootNode = new KdtBranchRootNode(avlSplitInfoParts._1, nodeAVLSplitInfo.pointCount)

        queueNodeInfo += ((kdtBranchRootNode, splitX, avlSplitInfoParts._2, avlSplitInfoParts._3))

        kdtBranchRootNode
      }
    }

    rootNode = buildNode(Histogram(iterPoints, histogramBarWidth, lowerBounds), splitX = true)

    while (queueNodeInfo.nonEmpty) {

      val (currNode, splitX, avlSplitInfoLeft, avlSplitInfoRight) = queueNodeInfo.dequeue

      if (avlSplitInfoLeft != null)
        currNode.left = buildNode(avlSplitInfoLeft, !splitX)
      if (avlSplitInfoRight != null)
        currNode.right = buildNode(avlSplitInfoRight, !splitX)
    }

    updateBoundsAndTotalPoint()

    true
  }

  override def findExact(searchXY: (Double, Double)): Point = {
    //if(searchXY._1.toInt==248 && searchXY._2.toInt==58)
    //  println
    val stackNode = mutable.Stack((this.rootNode, true))

    while (stackNode.nonEmpty) {

      val (currNode, splitX) = stackNode.pop

      currNode match {
        case kdtBRN: KdtBranchRootNode =>

          if (kdtBRN.splitVal == SPLIT_VAL_NONE)
            stackNode.push((kdtBRN.left, !splitX), (kdtBRN.right, !splitX))
          else if ((if (splitX) searchXY._1 else searchXY._2) <= kdtBRN.splitVal)
            stackNode.push((kdtBRN.left, !splitX))
          else
            stackNode.push((kdtBRN.right, !splitX))

        case kdtLeafNode: KdtLeafNode =>

          val optPoint = kdtLeafNode.lstPoints.find(pt => pt.x.equals(searchXY._1) && pt.y.equals(searchXY._2))

          if (optPoint.nonEmpty)
            return optPoint.get
      }
    }

    null
  }

  def printIndented(): Unit =
    printIndented(rootNode, "", isLeft = false)

  private def printIndented(node: KdtNode, indent: String, isLeft: Boolean): Unit = {

    if (node != null) {

      println(indent + (if (isLeft) "|__" else "|__") + node)

      node match {
        case kdtBRN: KdtBranchRootNode =>
          printIndented(kdtBRN.left, indent + (if (isLeft) "|  " else "   "), isLeft = true)
          printIndented(kdtBRN.right, indent + (if (isLeft) "|  " else "   "), isLeft = false)
        case _ =>
      }
    }
  }

  def findBestNode(searchPoint: Geom2D, minCount: Int): (KdtNode, Boolean) = {

    // find leaf containing point
    val fNodeCheck = (kdtNode: KdtNode) => kdtNode != null && kdtNode.totalPoints >= minCount && kdtNode.rectNodeBounds.contains(searchPoint)
    var currNode = rootNode
    var splitX = true

    while (currNode != null) {

      currNode match {
        case kdtBRN: KdtBranchRootNode =>

          if (kdtBRN.splitVal == SPLIT_VAL_NONE) {

            if (fNodeCheck(kdtBRN.left))
              currNode = kdtBRN.left
            else if (fNodeCheck(kdtBRN.right))
              currNode = kdtBRN.right
            else
              return (kdtBRN, splitX)
            //            // switch to node with larger side
            //            val sideLeft = if (fNodeCheck(kdtBRN.left)) math.max(kdtBRN.left.rectNodeBounds.halfXY.x, kdtBRN.left.rectNodeBounds.halfXY.y) else Double.NegativeInfinity
            //            val sideRight = if (fNodeCheck(kdtBRN.right)) math.max(kdtBRN.right.rectNodeBounds.halfXY.x, kdtBRN.right.rectNodeBounds.halfXY.y) else Double.NegativeInfinity
            //
            //            if (sideLeft == Double.NegativeInfinity && sideRight == Double.NegativeInfinity)
            //              return (kdtBRN, splitX)
            //
            //            currNode = if (sideLeft > sideRight) kdtBRN.left else kdtBRN.right
          }
          else if ((if (splitX) searchPoint.x else searchPoint.y) <= kdtBRN.splitVal)
            if (fNodeCheck(kdtBRN.left))
              currNode = kdtBRN.left
            else
              return (currNode, splitX)
          else if (fNodeCheck(kdtBRN.right))
            currNode = kdtBRN.right
          else
            return (currNode, splitX)

          splitX = !splitX

        case _: KdtNode =>
          return (currNode, splitX)
      }
    }

    null
  }

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedLinkedList[Point]) {

    //    if (searchPoint.userData.toString().equalsIgnoreCase("Taxi_1_A_3237"))
    //      println

    var (sPtBestNode, splitX) = findBestNode(searchPoint, sortSetSqDist.maxSize)

    val knnLookupInfo = new KnnLookupInfo(searchPoint, sortSetSqDist, sPtBestNode.rectNodeBounds)

    def process(kdtNode: KdtNode, skipKdtNode: KdtNode) {

      val queueKdtNode = mutable.Queue((kdtNode, splitX))

      while (queueKdtNode.nonEmpty) {

        val row = queueKdtNode.dequeue()

        if (row._1 != skipKdtNode)
          row._1 match {
            case kdtBRN: KdtBranchRootNode =>

              if (kdtBRN.left != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.left.rectNodeBounds))
                queueKdtNode += ((kdtBRN.left, !row._2))
              if (kdtBRN.right != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.right.rectNodeBounds))
                queueKdtNode += ((kdtBRN.right, !row._2))

            case kdtLeafNode: KdtLeafNode =>

              //              if(kdtLeafNode.lstPoints.find(_.userData.toString.equalsIgnoreCase("bus_1_b_291848")).nonEmpty)
              //                println

              if (knnLookupInfo.rectSearchRegion.intersects(kdtLeafNode.rectNodeBounds))
                kdtLeafNode.lstPoints.foreach(testAndAddPoint(_, knnLookupInfo))
          }
      }
    }

    process(sPtBestNode, null)

    if (sPtBestNode != this.rootNode) {

      splitX = true
      process(this.rootNode, sPtBestNode)
    }
  }

  override def toString: String =
    rootNode.toString

  override def write(kryo: Kryo, output: Output): Unit = {

    val lstNode = ListBuffer(this.rootNode)

    lstNode.foreach(kdtNode => {

      kryo.writeClassAndObject(output, kdtNode)

      kdtNode match {
        case brn: KdtBranchRootNode =>
          lstNode += (brn.left, brn.right)
        case _ =>
      }
    })
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    def readNode() = kryo.readClassAndObject(input) match {
      case kdtNode: KdtNode => kdtNode
    }

    this.rootNode = readNode()

    val lstNode = ListBuffer(this.rootNode)

    lstNode.foreach {
      case brn: KdtBranchRootNode =>
        brn.left = readNode()
        brn.right = readNode()

        lstNode += (brn.left, brn.right)
      case _ =>
    }
  }

  private def updateBoundsAndTotalPoint(): Unit =
    rootNode match {
      case kdtBRN: KdtBranchRootNode =>

        val stackNode = mutable.Stack[KdtBranchRootNode](kdtBRN)
        //        val stackRoots = mutable.Stack[KdtBranchRootNode]()

        while (stackNode.nonEmpty) {

          //          stackRoots.push(stackNode.pop)
          val currNode = stackNode.top

          currNode.left match {
            case kdtBRN: KdtBranchRootNode =>
              if (currNode.left.rectNodeBounds == null)
                stackNode.push(kdtBRN)
            case _ =>
          }

          currNode.right match {
            case kdtBRN: KdtBranchRootNode =>
              if (currNode.right.rectNodeBounds == null)
                stackNode.push(kdtBRN)
            case _ =>
          }

          if (currNode == stackNode.top) {

            stackNode.pop

            if (currNode.left != null)
              currNode.rectNodeBounds = new Rectangle(currNode.left.rectNodeBounds)

            if (currNode.right != null)
              if (currNode.rectNodeBounds == null)
                currNode.rectNodeBounds = new Rectangle(currNode.right.rectNodeBounds)
              else
                currNode.rectNodeBounds.mergeWith(currNode.right.rectNodeBounds)
          }
        }
      case _ =>
    }

  override def allPoints: Iterator[Iterator[Point]] = new AbstractIterator[Iterator[Point]] {

    private val queueNode = mutable.Queue[KdtNode](rootNode)

    override def hasNext: Boolean = queueNode.nonEmpty

    override def next(): Iterator[Point] =
      if (!hasNext)
        throw new NoSuchElementException("next on empty Iterator")
      else {

        var ans: Iterator[Point] = null

        while (ans == null && queueNode.nonEmpty)
          ans = this.queueNode.dequeue match {
            case kdtBRN: KdtBranchRootNode =>
              this.queueNode += (kdtBRN.left, kdtBRN.right)
              null
            case kdtLeafNode: KdtLeafNode =>
              kdtLeafNode.lstPoints.iterator
            case _ =>
              null
          }

        ans
      }
  }
}