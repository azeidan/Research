package org.cusp.bdi.ds.kdt

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.cusp.bdi.ds.SpatialIndex
import org.cusp.bdi.ds.SpatialIndex.{KnnLookupInfo, testAndAddPoint}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.KdTree.findSearchRegionLocation
import org.cusp.bdi.ds.sortset.SortedList

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object KdTree extends Serializable {

  def findSearchRegionLocation(searchRegion: Rectangle, nodeSplitVal: Double, splitX: Boolean): Char = {

    val limits = if (splitX)
      (searchRegion.left, searchRegion.right)
    else
      (searchRegion.bottom, searchRegion.top)

    nodeSplitVal match {
      case sk if sk < limits._1 => 'R' // region to the right (or below) of the splitKey
      case sk if sk > limits._2 => 'L' // region to the left (or above) of the splitKey
      case _ => 'B' // region contains the splitKey
    }
  }
}

class KdTree extends SpatialIndex {

  var rootNode: KdtNode = _

  //  private var rectBounds: Rectangle = _
  //  private var hgGroupWidth = -1

  def getTotalPoints: Int =
    rootNode.totalPoints

  def extractHGGroupWidth(rectBounds: Rectangle, iterPoints: Iterator[Point], otherInitializers: Seq[Any]) = {

    if (rootNode != null) throw new IllegalStateException("KD Tree already built")
    if (rectBounds == null) throw new IllegalStateException("Rectangle bounds cannot be null")
    if (iterPoints.isEmpty) throw new IllegalStateException("Empty point iterator")
    if (otherInitializers.length != 1) throw new IllegalStateException("%s%d".format("KdTree only accepts one additional initializer for Histogram operations. Got ", otherInitializers.length))

    otherInitializers.head match {
      case i: Int =>

        if (i < 1) throw new IllegalStateException("%s%d".format("Histogram bar width must be >= 1: Got: ", i))

        i
      case _ =>
        throw new IllegalStateException("Histogram bar width must be >= 1")
    }
  }

  @throws(classOf[IllegalStateException])
  override def insert(rectBounds: Rectangle, iterPoints: Iterator[Point], otherInitializers: Any*): Boolean = {

    val hgGroupWidth = extractHGGroupWidth(rectBounds, iterPoints, otherInitializers)

    val queueNode = mutable.Queue[(KdtBranchRootNode, Boolean, AVLSplitInfo, AVLSplitInfo)]()

    val lowerBounds = (rectBounds.left, rectBounds.bottom)

    def buildNode(nodeAVLSplitInfo: AVLSplitInfo, splitX: Boolean): KdtNode = {
      if (nodeAVLSplitInfo.canPartition) {

        val avlSplitInfoParts = nodeAVLSplitInfo.partition

        val splitVal = avlSplitInfoParts._1 * hgGroupWidth + (if (splitX) lowerBounds._1
        else lowerBounds._2) + hgGroupWidth - 1e-6

        val kdtBranchRootNode = new KdtBranchRootNode(splitVal, nodeAVLSplitInfo.pointCount)

        queueNode += ((kdtBranchRootNode, splitX, avlSplitInfoParts._2, avlSplitInfoParts._3))

        kdtBranchRootNode
      }
      else {

        val pointInf = nodeAVLSplitInfo.extractPointInfo()
        new KdtLeafNode(pointInf._1, pointInf._2)
      }
    }

    var avlSplitInfo = AVLSplitInfo(iterPoints, hgGroupWidth, lowerBounds)

    rootNode = buildNode(avlSplitInfo, splitX = true)
    avlSplitInfo = null

    while (queueNode.nonEmpty) {

      val (currNode, splitX, avlSplitInfoLeft, avlSplitInfoRight) = queueNode.dequeue()

      if (avlSplitInfoLeft != null)
        currNode.left = buildNode(avlSplitInfoLeft, !splitX)
      if (avlSplitInfoRight != null)
        currNode.right = buildNode(avlSplitInfoRight, !splitX)
    }

    updateBoundsAndTotalPoint()

    true
  }

  override def findExact(searchXY: (Double, Double)): Point = {

    var currNode = this.rootNode
    var splitX = true

    while (currNode != null)
      currNode match {
        case kdtBranchRootNode: KdtBranchRootNode =>

          currNode = if ((if (splitX) searchXY._1
          else searchXY._2) <= kdtBranchRootNode.splitVal)
            kdtBranchRootNode.left
          else
            kdtBranchRootNode.right

          splitX = !splitX

        case kdtLeafNode: KdtLeafNode =>
          return kdtLeafNode.lstPoints.find(pt => pt.x.equals(searchXY._1) && pt.y.equals(searchXY._2)).orNull
      }

    null
  }

  def findBestNode(searchPoint: Geom2D, k: Int): (KdtNode, Boolean) = {

    // find leaf containing point
    var currNode = rootNode
    var splitX = true

    while (true)
      currNode match {
        case kdtBRN: KdtBranchRootNode =>

          if ((if (splitX) searchPoint.x
          else searchPoint.y) <= kdtBRN.splitVal)
            if (kdtBRN.left != null && kdtBRN.left.totalPoints >= k)
              currNode = kdtBRN.left
            else
              return (currNode, splitX)
          else if (kdtBRN.right != null && kdtBRN.right.totalPoints >= k)
            currNode = kdtBRN.right
          else
            return (currNode, splitX)

          splitX = !splitX

        case _: KdtNode =>
          return (currNode, splitX)
      }

    null
  }

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point]) {

    var (sPtBestNode, splitX) = findBestNode(searchPoint, sortSetSqDist.maxSize)

    val knnLookupInfo = new KnnLookupInfo(searchPoint, sortSetSqDist, sPtBestNode.rectNodeBounds)

    def process(kdtNode: KdtNode, skipKdtNode: KdtNode) {

      val queueKdtNode = mutable.Queue((kdtNode, splitX))

      while (queueKdtNode.nonEmpty) {

        val row = queueKdtNode.dequeue()

        if (row._1 != skipKdtNode)
          row._1 match {
            case kdtBRN: KdtBranchRootNode =>

              findSearchRegionLocation(knnLookupInfo.rectSearchRegion, kdtBRN.splitVal, row._2) match {
                case 'L' =>
                  if (kdtBRN.left != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.left.rectNodeBounds))
                    queueKdtNode += ((kdtBRN.left, !row._2))
                case 'R' =>
                  if (kdtBRN.right != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.right.rectNodeBounds))
                    queueKdtNode += ((kdtBRN.right, !row._2))
                case _ =>
                  if (kdtBRN.left != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.left.rectNodeBounds))
                    queueKdtNode += ((kdtBRN.left, !row._2))
                  if (kdtBRN.right != null && knnLookupInfo.rectSearchRegion.intersects(kdtBRN.right.rectNodeBounds))
                    queueKdtNode += ((kdtBRN.right, !row._2))
              }

            case kdtLeafNode: KdtLeafNode =>
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
    "%s".format(rootNode)

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

  private def updateBoundsAndTotalPoint() {

    val startBranchRootNode = rootNode match {
      case kdtBranchRootNode: KdtBranchRootNode =>
        kdtBranchRootNode
      case _ =>
        return // nothing to do, exit method
    }

    val stackNode = mutable.Stack[KdtBranchRootNode](startBranchRootNode)
    val stackRoots = mutable.Stack[KdtBranchRootNode]()

    while (stackNode.nonEmpty) {

      stackRoots.push(stackNode.pop)

      stackRoots.top.left match {
        case kdtBranchRootNode: KdtBranchRootNode =>
          stackNode.push(kdtBranchRootNode)
        case _ =>
      }

      stackRoots.top.right match {
        case kdtBranchRootNode: KdtBranchRootNode =>
          stackNode.push(kdtBranchRootNode)
        case _ =>
      }
    }

    stackRoots.foreach(currNode => {

      if (currNode.left != null)
        currNode.rectNodeBounds = new Rectangle(currNode.left.rectNodeBounds)

      if (currNode.right != null)
        if (currNode.rectNodeBounds == null)
          currNode.rectNodeBounds = new Rectangle(currNode.right.rectNodeBounds)
        else
          currNode.rectNodeBounds.mergeWith(currNode.right.rectNodeBounds)
    })
  }
}