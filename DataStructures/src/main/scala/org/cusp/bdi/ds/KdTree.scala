package org.cusp.bdi.ds

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.KdTree.nodeCapacity
import org.cusp.bdi.ds.SpatialIndex.{calcListInfo, computeDimension, testAndAddPoint}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object KdTree extends Serializable {

  val nodeCapacity = 4
}

class KdtNode extends KryoSerializable {

  var totalPoints: Int = 0
  var iterPoints: Iterable[Point] = _
  var rectBounds: Rectangle = _

  def this(map: Map[Double, Map[Double, ListBuffer[Point]]]) = {

    this()
    extractPointIter(map)
  }

  protected def extractPointIter(map: Map[Double, Map[Double, ListBuffer[Point]]]): Unit = {

    this.iterPoints = map.values.map(_.values).flatMap(_.seq).flatMap(_.seq)
  }

  override def toString: String =
    "%s\t%d".format(rectBounds, totalPoints)

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeInt(totalPoints)
    kryo.writeClassAndObject(output, iterPoints)
    kryo.writeClassAndObject(output, rectBounds)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    totalPoints = input.readInt()
    iterPoints = kryo.readClassAndObject(input).asInstanceOf[ListBuffer[Point]]
    rectBounds = kryo.readClassAndObject(input) match {
      case rectangle: Rectangle => rectangle
    }
  }
}

final class KdtBranchRootNode extends KdtNode {

  var splitKey: Double = 0
  var left: KdtNode = _
  var right: KdtNode = _

  def this(map: Map[Double, Map[Double, ListBuffer[Point]]]) = {

    this()
    extractPointIter(map)
  }

  override def toString: String = {

    val strR = if (left == null) 'X' else '\\'
    val strL = if (left == null) 'X' else '/'

    "%s %s %s".format(super.toString(), strL, strR)
  }
}

class KdTree(barWidth: Int) extends SpatialIndex {

  var root: KdtNode = _

  def getTotalPoints: Int = root.totalPoints

  override def insert(lstPoints: ListBuffer[Point]): Boolean = {

    if (root != null)
      throw new IllegalStateException("KD Tree already built")

    if (lstPoints.isEmpty)
      throw new IllegalStateException("Empty point iterator")

    if (barWidth < 1)
      throw new IllegalStateException("Bar width must be >= 1")

    //    if (arrPoints.filter(_.userData.toString().equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
    //      println

    val mapHG: Map[Double, Map[Double, ListBuffer[Point]]] = lstPoints.map(pt =>
      if (barWidth == 1)
        (math.floor(pt.x), (math.floor(pt.y), pt))
      else
        (math.floor(pt.x / barWidth), (math.floor(pt.y / barWidth), pt))
    )
      .groupBy(_._1)
      .mapValues(_.map(_._2)
        .groupBy(_._1)
        .mapValues(_.map(_._2)))

    val lstNodeInfo = ListBuffer[(KdtBranchRootNode, Boolean, Map[Double, Map[Double, ListBuffer[Point]]], Map[Double, Map[Double, ListBuffer[Point]]])]()

    def buildNode(mapHG_node: Map[Double, Map[Double, ListBuffer[Point]]], splitX: Boolean) =

      mapHG_node.size match {
        case 0 =>
          null
        case s if s <= nodeCapacity =>
          new KdtNode(mapHG_node)
        case _ =>

          val splitKey = if (splitX)
            mapHG_node.mapValues(_.values.map(_.size).sum).maxBy(_._2)._1
          else
            mapHG_node.values.map(_.mapValues(_.size)).flatMap(_.seq).groupBy(_._1).maxBy(_._2.size)._1

          val mapParts = threeWayPartition(mapHG_node, splitKey, splitX)

          if (mapParts._1.isEmpty && mapParts._3.isEmpty)
            new KdtNode(mapParts._2)
          else {

            val kdtBranchRootNode = new KdtBranchRootNode(mapParts._2)
            kdtBranchRootNode.splitKey = splitKey

            lstNodeInfo += ((kdtBranchRootNode, splitX, mapParts._1, mapParts._3))

            kdtBranchRootNode
          }
      }

    root = buildNode(mapHG, splitX = true)

    lstNodeInfo.foreach(row => {

      val (currNode, splitX, mapLeft, mapRight) = row

      currNode.left = buildNode(mapLeft, !splitX)
      currNode.right = buildNode(mapRight, !splitX)
    })

    updateNodeBoundsAndTotals()

    true
  }

  override def findExact(searchXY: (Double, Double)): Point = {

    var currNode = this.root

    def checkNodePoints() = {

      val lst = currNode.iterPoints.filter(pt => pt.x.equals(searchXY._1) && pt.y.equals(searchXY._2)).take(1)

      if (lst.nonEmpty)
        lst.head
      else
        null
    }

    val searchXY_grid = if (barWidth == 1) (math.floor(searchXY._1), math.floor(searchXY._2)) else (math.floor(searchXY._1 / barWidth), math.floor(searchXY._2 / barWidth))
    var splitX = true

    while (currNode != null)
      currNode match {
        case kdtBRN: KdtBranchRootNode =>

          (if (splitX) searchXY_grid._1 else searchXY_grid._2) match {
            case xy if xy == kdtBRN.splitKey => return checkNodePoints()
            case xy if xy < kdtBRN.splitKey => currNode = kdtBRN.left
            case _ => currNode = kdtBRN.right
          }
          splitX = !splitX
        case _ =>
          return checkNodePoints()
      }

    null
  }

  def findBestNode(searchPoint: Geom2D, k: Int): KdtNode = {

    // find leaf containing point
    var currNode = root
    val searchXY_grid = if (barWidth == 1) (math.floor(searchPoint.x), math.floor(searchPoint.y)) else (math.floor(searchPoint.x / barWidth), math.floor(searchPoint.y / barWidth))
    var splitX = true
    var found = false

    while (!found)
      currNode match {
        case kdtBRN: KdtBranchRootNode =>

          (if (splitX) searchXY_grid._1 else searchXY_grid._2) match {
            case xy if xy == kdtBRN.splitKey =>
              found = true
            case xy if xy < kdtBRN.splitKey =>
              if (kdtBRN.left != null && kdtBRN.left.totalPoints >= k && kdtBRN.left.rectBounds.contains(searchPoint))
                currNode = kdtBRN.left
              else
                found = true
            case _ =>
              if (kdtBRN.right != null && kdtBRN.right.totalPoints >= k && kdtBRN.right.rectBounds.contains(searchPoint))
                currNode = kdtBRN.right
              else
                found = true
          }
          splitX = !splitX

        case _: KdtNode =>
          found = true
      }

    currNode
  }

  def printInOrder(): Unit =
    printInOrder(root, "")

  def printInOrder(node: KdtNode, delimiter: String): Unit =
    if (node != null) {

      println("%s%s".format(delimiter, node))

      node match {
        case brn: KdtBranchRootNode =>
          printInOrder(brn.left, delimiter + "\t")
          printInOrder(brn.right, delimiter + "\t")
        case _ =>
      }
    }

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point]) {

    //    if (searchPoint.userData.toString.equalsIgnoreCase("bread_3_a_822279"))
    //      println()

    val sPtBestNode = findBestNode(searchPoint, sortSetSqDist.maxSize)

    val dim = if (sortSetSqDist.isFull)
      math.sqrt(sortSetSqDist.last.distance)
    else
      computeDimension(searchPoint, sPtBestNode.rectBounds)

    val searchRegion = Rectangle(searchPoint, new Geom2D(dim))

    var prevMaxSqrDist = if (sortSetSqDist.last == null) -1 else sortSetSqDist.last.distance

    def process(kdtNode: KdtNode, skipKdtNode: KdtNode) {

      val lstNodeInf = ListBuffer(kdtNode)

      lstNodeInf.foreach(node =>
        //        if (searchRegion.center match {
        //          case pt: Point => pt.userData.toString().equalsIgnoreCase("bread_3_a_822279")
        //          case _ => false
        //        })
        //          if (node.arrPoints.filter(_.userData.toString().equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
        //        println

        if (node != skipKdtNode)
          if (searchRegion.intersects(node.rectBounds)) {

            node.iterPoints.foreach(pt => prevMaxSqrDist = testAndAddPoint(pt, searchRegion, sortSetSqDist, prevMaxSqrDist))

            node match {
              case brn: KdtBranchRootNode =>

                if (brn.left != null)
                  lstNodeInf += brn.left

                if (brn.right != null)
                  lstNodeInf += brn.right
              case _ =>
            }
          }
      )
    }

    process(sPtBestNode, null)

    if (sPtBestNode != this.root)
      process(this.root, sPtBestNode)
  }

  private def threeWayPartition(mapHG_node: Map[Double, Map[Double, ListBuffer[Point]]], splitVal_key: Double, splitX: Boolean) = {

    if (splitX) {

      val (mapLT_EQ, mapGT) = mapHG_node.partition(_._1 <= splitVal_key)

      val (mapEQ, mapLT) = mapLT_EQ.partition(_._1 == splitVal_key)

      (mapLT, mapEQ, mapGT)
    } else {

      var mapParts = mapHG_node.mapValues(_.partition(_._1 <= splitVal_key))
      val mapLT_EQ = mapParts.mapValues(_._1).filter(_._2.nonEmpty)
      val mapGT = mapParts.mapValues(_._2).filter(_._2.nonEmpty)

      mapParts = mapLT_EQ.mapValues(_.partition(_._1 < splitVal_key))
      val mapLT = mapParts.mapValues(_._1).filter(_._2.nonEmpty)
      val mapEQ = mapParts.mapValues(_._2).filter(_._2.nonEmpty)

      (mapLT, mapEQ, mapGT)
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

    while (queueNode.nonEmpty)
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

  private def updateNodeBoundsAndTotals() {

    def processNode(kdtNode: KdtNode) {

      val nodeInfo = calcListInfo(kdtNode.iterPoints)
      kdtNode.totalPoints = nodeInfo._1
      kdtNode.rectBounds = nodeInfo._2
    }

    val stackKdtNode = mutable.Stack(this.root)

    while (stackKdtNode.nonEmpty)
      stackKdtNode.top match {
        case kdtBRN: KdtBranchRootNode =>
          if (kdtBRN.left != null && kdtBRN.left.rectBounds == null)
            stackKdtNode.push(kdtBRN.left)
          else if (kdtBRN.right != null && kdtBRN.right.rectBounds == null)
            stackKdtNode.push(kdtBRN.right)
          else {

            processNode(kdtBRN)

            if (kdtBRN.left != null) {

              kdtBRN.rectBounds.mergeWith(kdtBRN.left.rectBounds)
              kdtBRN.totalPoints += kdtBRN.left.totalPoints
            }

            if (kdtBRN.right != null) {

              kdtBRN.rectBounds.mergeWith(kdtBRN.right.rectBounds)
              kdtBRN.totalPoints += kdtBRN.right.totalPoints
            }

            stackKdtNode.pop()
          }
        case kdtNode: KdtNode =>
          processNode(kdtNode)
          stackKdtNode.pop()
      }
  }
}