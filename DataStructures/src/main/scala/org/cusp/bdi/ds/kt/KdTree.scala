package org.cusp.bdi.ds.kt

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.kt.KdTree.{computeRectMBR, nodeCapacity}
import org.cusp.bdi.ds.{Geom2D, Point, Rectangle}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object KdTree extends Serializable {

  val nodeCapacity = 4

  //  val SER_MARKER_NULL: Byte = Byte.MinValue
  //  val SER_MARKER_BRANCH_ROOT_NODE: Byte = 0.toByte
  //  val SER_MARKER_LEAF_NODE: Byte = 1.toByte

  def computeRectMBR(arrPoints: Array[Point]): Rectangle = {

    if (arrPoints == null)
      null
    else {

      val ends = arrPoints
        .map(point => (point.x, point.y, point.x, point.y))
        .reduce((mbr1, mbr2) => (math.min(mbr1._1, mbr2._1), math.min(mbr1._2, mbr2._2), math.max(mbr1._3, mbr2._3), math.max(mbr1._4, mbr2._4)))

      val halfXY = new Geom2D(((ends._3 - ends._1) + 1) / 2, ((ends._4 - ends._2) + 1) / 2)

      Rectangle(new Geom2D(ends._1 + halfXY.x, ends._2 + halfXY.y), halfXY)
    }
  }
}

class KdtNode(_arrPoints: Array[Point]) extends KryoSerializable {

  var totalPoints = 0
  var arrPoints: Array[Point] = _arrPoints
  var rectMBR: Rectangle = _

  override def toString: String =
    "%s\t%d".format(rectMBR, arrPoints.length)

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

final class KdtBranchRootNode(arrPoints: Array[Point]) extends KdtNode(arrPoints) {

  var arrSplitPoints: Array[Point] = null //if (splitX) arrPoints(arrPoints.length / 2).x else arrPoints(arrPoints.length / 2).y // if (arrPoints == null) 0 else arrPoints.map(if (splitX) _.x else _.y).sum / arrPoints.size
  var left: KdtNode = _
  var right: KdtNode = _

  override def toString: String = {

    val strR = if (left == null) 'X' else '\\'
    val strL = if (left == null) 'X' else '/'

    "%s\t%s\t%s".format(super.toString, strL, strR)
  }

  override def write(kryo: Kryo, output: Output): Unit = {

    super.write(kryo, output)
    kryo.writeClassAndObject(output, arrSplitPoints)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    super.read(kryo, input)
    arrSplitPoints = kryo.readClassAndObject(input).asInstanceOf[Array[Point]]
  }
}

object TestKdTree {

  def main(args: Array[String]): Unit = {

    val arrPoint = Array(new Point(0.54, 0.93), new Point(0.96, 0.86), new Point(0.42, 0.67), new Point(0.11, 0.53), new Point(0.64, 0.29), new Point(0.27, 0.75), new Point(0.81, 0.63))

    val kdt = new KdTree()
    kdt.insert(arrPoint)
    kdt.printInOrder()

    println(kdt.findExact((9, 1)))
  }
}

class KdTree extends KryoSerializable {

  var root: KdtNode = _

  def getTotalPoints: Int = root.totalPoints

  def threeWayPartition(arrPointsNode: Array[Point], splitVal: Double, splitX: Boolean) = {

    val lstLT = ListBuffer[Point]()
    val lstEQ = ListBuffer[Point]()
    val lstGT = ListBuffer[Point]()

    arrPointsNode.foreach(_ match {
      case p if (if (splitX) p.x else p.y) < splitVal => lstLT += p
      case p if (if (splitX) p.x else p.y) == splitVal => lstEQ += p
      case p => lstGT += p
    })

    (lstLT.toArray, lstEQ.toArray, lstGT.toArray)
  }

  def insert(arrPoints: Array[Point]): Boolean = {

    if (root != null)
      throw new IllegalStateException("KD Tree already built")

    if (arrPoints.size <= nodeCapacity)
      root = new KdtNode(arrPoints)
    else
      root = new KdtBranchRootNode(arrPoints)

    val queueProcess = mutable.Queue((root, true, arrPoints))

    while (queueProcess.nonEmpty) {

      val (kdtNode, splitX, arrPointsNode) = queueProcess.dequeue

      kdtNode.totalPoints = arrPointsNode.length

      kdtNode match {
        case node: KdtNode =>
          node.rectMBR = computeRectMBR(arrPointsNode)
        case brn: KdtBranchRootNode =>

          val splitVal = if (splitX) arrPointsNode(arrPointsNode.length / 2).x else arrPointsNode(arrPointsNode.length / 2).y

          val (arrPointsLT, arrPointsEQ, arrPointsGT) = threeWayPartition(arrPointsNode, splitVal, splitX)

          brn.arrPoints = arrPointsEQ

          if (arrPointsLT.length <= nodeCapacity)
            brn.left = new KdtNode(arrPointsLT)
          else
            brn.left = new KdtBranchRootNode(arrPointsLT)

          if (arrPointsGT.length <= nodeCapacity)
            brn.right = new KdtNode(arrPointsGT)
          else
            brn.right = new KdtBranchRootNode(arrPointsGT)

          queueProcess += ((brn.left, !splitX, arrPointsLT), (brn.right, !splitX, arrPointsGT))
      }
    }

    // update MBRs
    updateMBR(this.root)

    def updateMBR(kdtNode: KdtNode): Rectangle =
      kdtNode match {
        case brn: KdtBranchRootNode =>
          val mbr1 = updateMBR(brn.left)
          val mbr2 = updateMBR(brn.right)
          brn.rectMBR = new Rectangle(mbr1)
          brn.rectMBR.mergeWith(mbr2)

          brn.rectMBR
        case node: KdtNode =>
          node.rectMBR
      }

    true
  }

  def findExact(searchXY: (Double, Double)): Point = {

    var currNode = this.root
    var checkX = true

    while (currNode != null) {
      currNode match {
        case nd: KdtNode =>

          if (nd.rectMBR.contains(searchXY._1, searchXY._2)) {

            val lst = nd.arrPoints.filter(qtPoint => searchXY._1.equals(qtPoint.x) && searchXY._2.equals(qtPoint.y)).take(1)

            if (lst.nonEmpty)
              return lst.head
          }
          else
            currNode = null

        case brn: KdtBranchRootNode =>
          if (brn.left != null && (if (checkX) searchXY._1 else searchXY._2) < brn.splitVal)
            currNode = brn.left
          else if (brn.right != null)
            currNode = brn.right
          else
            currNode = null
      }

      checkX = !checkX
    }

    null
  }

  def printInOrder(): Unit =
    printInOrder(root, "")

  def printInOrder(node: KdtNode, delimiter: String): Unit = {

    if (node != null) {

      node match {
        case ln: KdtLeafNode =>
          println("%s%s".format(delimiter, ln))
        case brn: KdtBranchRootNode =>
          printInOrder(brn.left, delimiter + "\t")
          printInOrder(brn.right, delimiter + "\t")
      }
    }
  }

  override def toString: String =
    "%s".format(root)

  override def write(kryo: Kryo, output: Output): Unit = {

    println(">>> kdt serialization start " + root)

    val queueNode = mutable.Queue(this.root)

    while (queueNode.nonEmpty) {

      val kdtNode = queueNode.dequeue()

      kryo.writeClassAndObject(output, kdtNode)

      kdtNode match {
        case brn: KdtBranchRootNode =>
          queueNode += (brn.left, brn.right)
        case _ => {
        }
      }
    }

    println(">>> kdt serialization done " + root)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    println(">>> kdt de-serialization start " + root)

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
        case _ => {
        }
      }
    }

    println(">>> kdt de-serialization end " + root)
  }
}