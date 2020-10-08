package org.cusp.bdi.ds

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.KdTree.{computeMBR, nodeCapacity}
import org.cusp.bdi.ds.SpatialIndex.testAndAddPoint
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object KdTree extends Serializable {

  val nodeCapacity = 4

  def computeMBR(arrPoints: Array[Point]): Rectangle = {

    //    if (arrPoints == null || arrPoints.length == 0)
    //      null
    //    else {

    val ends = arrPoints
      .map(point => (point.x, point.y, point.x, point.y))
      .reduce((mbr1, mbr2) => (math.min(mbr1._1, mbr2._1), math.min(mbr1._2, mbr2._2), math.max(mbr1._3, mbr2._3), math.max(mbr1._4, mbr2._4)))

    val halfXY = new Geom2D(((ends._3 - ends._1) + 1) / 2, ((ends._4 - ends._2) + 1) / 2)

    Rectangle(new Geom2D(ends._1 + halfXY.x, ends._2 + halfXY.y), halfXY)
    //    }
  }
}

class KdtNode(_arrPoints: Array[Point]) extends KryoSerializable {

  var arrPoints: Array[Point] = _arrPoints
  var totalPoints: Int = if (_arrPoints == null) 0 else _arrPoints.length
  var rectMBR: Rectangle = if (_arrPoints == null) null else computeMBR(_arrPoints)

  override def toString: String =
    "%s\t%d\t%d".format(rectMBR, arrPoints.length, totalPoints)

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeInt(totalPoints)
    kryo.writeClassAndObject(output, arrPoints)
    kryo.writeClassAndObject(output, rectMBR)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    totalPoints = input.readInt()
    arrPoints = kryo.readClassAndObject(input).asInstanceOf[Array[Point]]
    rectMBR = kryo.readClassAndObject(input) match {
      case rectangle: Rectangle => rectangle
    }
  }
}

final class KdtBranchRootNode(arrPoints: Array[Point]) extends KdtNode(null) {

  var left: KdtNode = _
  var right: KdtNode = _

  totalPoints = arrPoints.length

  override def toString: String = {

    val strR = if (left == null) 'X' else '\\'
    val strL = if (left == null) 'X' else '/'

    "%s\t%s\t%s".format(super.toString, strL, strR)
  }
}

class KdTree extends SpatialIndex {

  var root: KdtNode = _

  def getTotalPoints: Int = root.totalPoints

  def threeWayPartition(arrPointsNode: Array[Point], splitX: Boolean): Array[Array[Point]] = {

    val arrSplitLists = Array.fill(3) {
      ListBuffer[Point]()
    }

    val splitVal = if (splitX) arrPointsNode(arrPointsNode.length / 2).x else arrPointsNode(arrPointsNode.length / 2).y

    arrPointsNode.foreach {
      case p if (if (splitX) p.x else p.y) == splitVal => arrSplitLists(0) += p
      case p if (if (splitX) p.x else p.y) < splitVal => arrSplitLists(1) += p
      case p => arrSplitLists(2) += p
    }

    // 0 -> equals
    // 1 -> <
    // 2 -> >
    arrSplitLists.map(_.toArray)
  }

  override def insert(iterPoints: Iterator[Point]): Boolean =
    insert(iterPoints.toArray)

  private def insert(arrPoints: Array[Point]): Boolean = {

    if (root != null)
      throw new IllegalStateException("KD Tree already built")

    //    if (arrPoints.filter(_.userData.toString().equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
    //      println

    if (arrPoints.length <= nodeCapacity)
      root = new KdtNode(arrPoints)
    else {

      var kdtBranchRootNode = new KdtBranchRootNode(arrPoints)

      root = kdtBranchRootNode

      val queueProcess = mutable.Queue((kdtBranchRootNode, true, arrPoints))

      while (queueProcess.nonEmpty) {

        val (currNode, splitX, arrPointsNode) = queueProcess.dequeue

        // [EQ, LT, GT]
        val arrSplitLists = threeWayPartition(arrPointsNode, splitX)

        //        if (arrSplitLists(0).filter(_.userData.toString().equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
        //          println

        currNode.arrPoints = arrSplitLists(0)
        currNode.rectMBR = computeMBR(arrSplitLists(0))

        //        if (arrSplitLists(0).filter(_.userData.toString.equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
        //          println

        if (arrSplitLists(1).nonEmpty)
          if (arrSplitLists(1).length <= nodeCapacity) {

            currNode.left = new KdtNode(arrSplitLists(1))

            //            if (arrSplitLists(1).filter(_.userData.toString().equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
            //              println
          } else {

            kdtBranchRootNode = new KdtBranchRootNode(arrSplitLists(1))
            currNode.left = kdtBranchRootNode
            queueProcess += ((kdtBranchRootNode, !splitX, arrSplitLists(1)))
          }

        if (arrSplitLists(2).nonEmpty)
          if (arrSplitLists(2).length <= nodeCapacity) {

            currNode.right = new KdtNode(arrSplitLists(2))

            //            if (arrSplitLists(2).filter(_.userData.toString().equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
            //              println
          } else {

            kdtBranchRootNode = new KdtBranchRootNode(arrSplitLists(2))
            currNode.right = kdtBranchRootNode
            queueProcess += ((kdtBranchRootNode, !splitX, arrSplitLists(2)))
          }
      }
    }

    // update MBRs
    updateMBR(this.root)

    //    tmp(this.root)
    //
    //    def tmp(nd: KdtNode): Unit = {
    //      nd match {
    //        case kdtBranchRootNode: KdtBranchRootNode => {
    //
    //          if (kdtBranchRootNode.left != null)
    //            if (kdtBranchRootNode.left.arrPoints.filter(_.userData.toString.equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
    //              println
    //
    //          if (kdtBranchRootNode.left != null)
    //            tmp(kdtBranchRootNode.left)
    //          if (kdtBranchRootNode.right != null)
    //            tmp(kdtBranchRootNode.right)
    //        }
    //        case _ =>
    //      }
    //    }

    true
  }

  def findExact(searchXY: (Double, Double)): Point = {

    var currNode = this.root
    var checkX = true

    while (currNode != null) {
      currNode match {
        case brn: KdtBranchRootNode =>
          if (if (checkX) searchXY._1 == brn.arrPoints.head.x else searchXY._2 == brn.arrPoints.head.y) {
            // checkX reversed for exact XY lookup
            return brn.arrPoints.filter(pt => if (checkX) searchXY._2 == pt.y else searchXY._1 == pt.x).take(1).head
          }
          else if (if (checkX) searchXY._1 < brn.arrPoints.head.x else searchXY._2 < brn.arrPoints.head.y)
            currNode = brn.left
          else
            currNode = brn.right
        case nd: KdtNode =>

          if (nd.rectMBR.contains(searchXY._1, searchXY._2)) {

            val lst = nd.arrPoints.filter(qtPoint => searchXY._1.equals(qtPoint.x) && searchXY._2.equals(qtPoint.y)).take(1)

            if (lst.nonEmpty)
              return lst.head
          }
          else
            currNode = null
      }

      checkX = !checkX
    }

    null
  }

  def printInOrder(): Unit =
    printInOrder(root, "")

  def printInOrder(node: KdtNode, delimiter: String): Unit =
    if (node != null) {

      println("%s%s[%s]".format(delimiter, node, node.arrPoints.mkString(",")))

      node match {
        case brn: KdtBranchRootNode =>
          printInOrder(brn.left, delimiter + "\t")
          printInOrder(brn.right, delimiter + "\t")
        case _ =>
      }
    }

  override def toString: String =
    "%s".format(root)

  override def write(kryo: Kryo, output: Output): Unit = {

    val queueNode = mutable.Queue(this.root)

    while (queueNode.nonEmpty) {

      val kdtNode = queueNode.dequeue()

      kryo.writeClassAndObject(output, kdtNode)

      kdtNode match {
        case brn: KdtBranchRootNode =>
          queueNode += (brn.left, brn.right)
        case _ =>
      }
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    this.root = kryo.readClassAndObject(input) match {
      case kdtNode: KdtNode => kdtNode
    }

    val queueNode = mutable.Queue(this.root)

    while (queueNode.nonEmpty) {

      queueNode.dequeue() match {
        case brn: KdtBranchRootNode =>
          brn.left = kryo.readClassAndObject(input) match {
            case kdtNode: KdtNode => kdtNode
          }
          brn.right = kryo.readClassAndObject(input) match {
            case kdtNode: KdtNode => kdtNode
          }

          queueNode += (brn.left, brn.right)
        case _ =>
      }
    }
  }

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point]) {

    var searchRegion: Rectangle = null
    var sPtBestNode: KdtNode = null

    if (searchPoint.userData.toString().equalsIgnoreCase("bread_3_a_822279"))
      println()

    sPtBestNode = getBestNode(searchPoint, sortSetSqDist.maxSize)

    val dim =
      if (sortSetSqDist.isFull)
        math.sqrt(sortSetSqDist.last.distance)
      else
        math.max(math.max(math.abs(searchPoint.x - sPtBestNode.rectMBR.left), math.abs(searchPoint.x - sPtBestNode.rectMBR.right)),
          math.max(math.abs(searchPoint.y - sPtBestNode.rectMBR.bottom), math.abs(searchPoint.y - sPtBestNode.rectMBR.top)))

    searchRegion = Rectangle(searchPoint, new Geom2D(dim, dim))

    pointsWithinRegion(sPtBestNode, searchRegion, sortSetSqDist)
  }

  private def pointsWithinRegion(sPtBestNode: KdtNode, searchRegion: Rectangle, sortSetSqDist: SortedList[Point]) {

    var prevMaxSqrDist = if (sortSetSqDist.last == null) -1 else sortSetSqDist.last.distance

    def process(kdtNode: KdtNode, skipBranchRootNode: KdtNode) {

      val stackNode = mutable.Stack(kdtNode)

      while (stackNode.nonEmpty) {

        val node = stackNode.pop

        //        if (searchRegion.center match {
        //          case pt: Point => pt.userData.toString().equalsIgnoreCase("bread_3_a_822279")
        //          case _ => false
        //        })
        //          if (node.arrPoints.filter(_.userData.toString().equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
        //        println

        if (node != skipBranchRootNode && node.rectMBR.intersects(searchRegion)) {

          node.arrPoints.foreach(pt => prevMaxSqrDist = testAndAddPoint(pt, searchRegion, sortSetSqDist, prevMaxSqrDist))

          node match {
            case brn: KdtBranchRootNode =>

              if (brn.left != null && brn.left.rectMBR.intersects(searchRegion)) {

                stackNode.push(brn.left)

                //                if (brn.left.arrPoints.filter(_.userData.toString().equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
                //                  println
              }

              if (brn.right != null && brn.right.rectMBR.intersects(searchRegion))
                stackNode.push(brn.right)

            case _ =>
          }
        }
      }
    }

    if (sPtBestNode != null)
      process(sPtBestNode, null)

    if (sPtBestNode != this.root)
      process(this.root, sPtBestNode)
  }

  def getBestNode(searchPoint: Geom2D, k: Int): KdtNode = {

    // find leaf containing point
    var nodeCurr = root

    def testNode(kdtNode: KdtNode): Boolean =
      kdtNode != null && kdtNode.totalPoints >= k && kdtNode.rectMBR.contains(searchPoint)

    var splitX = true

    while (true) {
      if ((if (splitX) nodeCurr.arrPoints.head.x else nodeCurr.arrPoints.head.y).equals(if (splitX) searchPoint.x else searchPoint.y))
        return nodeCurr
      else
        nodeCurr match {

          case brn: KdtBranchRootNode =>
            if (testNode(brn.left))
              nodeCurr = brn.left
            else if (testNode(brn.right))
              nodeCurr = brn.right
            else
              return nodeCurr

          case node: KdtNode =>
            return node
        }

      splitX = !splitX
    }

    null
  }

  private def updateMBR(kdtNode: KdtNode): Rectangle = {
    kdtNode match {
      case brn: KdtBranchRootNode =>
        brn.rectMBR
          .mergeWith(if (brn.left == null) null else updateMBR(brn.left))
          .mergeWith(if (brn.right == null) null else updateMBR(brn.right))
      case _: KdtNode =>
    }

    kdtNode.rectMBR
  }
}