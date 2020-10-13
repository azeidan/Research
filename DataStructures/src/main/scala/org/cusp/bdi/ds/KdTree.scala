package org.cusp.bdi.ds

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.KdTree.{nodeCapacity, testNode}
import org.cusp.bdi.ds.SpatialIndex.{buildRectBounds, computeBounds, computeDimension, testAndAddPoint}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}

import scala.collection.mutable

object KdTree extends Serializable {

  val nodeCapacity = 4

  def testNode(brn: KdtBranchRootNode, checkX: Boolean, searchRegion: Rectangle): Boolean =
    if (checkX)
      brn.lstPoints.head.x >= searchRegion.left && brn.lstPoints.head.x <= searchRegion.right
    else
      brn.lstPoints.head.y >= searchRegion.bottom && brn.lstPoints.head.y <= searchRegion.top
}

class KdtNode() extends KryoSerializable {

  var totalPoints: Int = 0
  var lstPoints: List[Point] = _
  var rectBounds: Rectangle = _

  def this(totalPoints: Int) = {
    this()
    this.totalPoints = totalPoints
  }

  def this(arrPoints: List[Point]) = {
    this(arrPoints.size)
    this.lstPoints = arrPoints
  }

  override def toString: String =
    "%d".format(totalPoints)

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeInt(totalPoints)
    kryo.writeClassAndObject(output, lstPoints)
    kryo.writeClassAndObject(output, rectBounds)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    totalPoints = input.readInt()
    lstPoints = kryo.readClassAndObject(input).asInstanceOf[List[Point]]
    rectBounds = kryo.readClassAndObject(input) match {
      case rectangle: Rectangle => rectangle
    }
  }
}

final class KdtBranchRootNode(totalPoints: Int) extends KdtNode(totalPoints) {

  var left: KdtNode = _
  var right: KdtNode = _

  override def toString: String = {

    val strR = if (left == null) 'X' else '\\'
    val strL = if (left == null) 'X' else '/'

    "%s %s %s".format(super.toString(), strL, strR)
  }
}

class KdTree(barWidth: Int) extends SpatialIndex {

  var root: KdtNode = _

  def getTotalPoints: Int = root.totalPoints

  override def insert(iterPoints: Iterator[Point]): Boolean = {

    if (root != null)
      throw new IllegalStateException("KD Tree already built")

    if (barWidth < 1)
      throw new IllegalStateException("bar width must be >= 1")

    //    if (arrPoints.filter(_.userData.toString().equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
    //      println

    var pointCount = 0
    val mapHG: Map[Double, Map[Double, List[Point]]] = iterPoints.map(pt => {

      pointCount += 1

      (math.floor(pt.x / barWidth), (math.floor(pt.y / barWidth), pt))
    })
      .toList
      .groupBy(_._1)
      .mapValues(_.map(_._2)
        .groupBy(_._1)
        .mapValues(_.map(_._2)))

    if (pointCount <= nodeCapacity)
      root = new KdtNode(mapHG.values.flatMap(_.seq).map(_._2).flatMap(_.seq).toList)
    else {

      var kdtBranchRootNode = new KdtBranchRootNode(pointCount)

      root = kdtBranchRootNode

      val queueProcess = mutable.Queue((kdtBranchRootNode, mapHG))

      while (queueProcess.nonEmpty) {

        val (currNode, mapHG_node) = queueProcess.dequeue

        val arrKeys = mapHG_node.keySet.toArray.sorted
        val splitVal_key = arrKeys(arrKeys.length / 2)

        val mapHG_parts = threeWayPartition(mapHG_node, splitVal_key)

        currNode.lstPoints = mapHG_parts._2.values.flatMap(_.seq).map(_._2).flatMap(_.seq).toList

        val sizePart1 = mapHG_parts._1.size //.map(_._2.size).sum

        if (sizePart1 > 0)
          if (sizePart1 <= nodeCapacity)
            currNode.left = new KdtNode(mapHG_parts._1.values.flatMap(_.seq).map(_._2).flatMap(_.seq).toList)
          else {

            kdtBranchRootNode = new KdtBranchRootNode(mapHG_parts._1.map(_._2.map(_._2.size).sum).sum)
            currNode.left = kdtBranchRootNode
            queueProcess += ((kdtBranchRootNode, reverseKeys(mapHG_parts._1)))
          }

        val sizePart2 = mapHG_parts._3.size

        if (sizePart2 > 0)
          if (sizePart2 <= nodeCapacity)
            currNode.right = new KdtNode(mapHG_parts._3.values.flatMap(_.seq).map(_._2).flatMap(_.seq).toList)
          else {

            kdtBranchRootNode = new KdtBranchRootNode(mapHG_parts._3.map(_._2.map(_._2.size).sum).sum)
            currNode.right = kdtBranchRootNode
            queueProcess += ((kdtBranchRootNode, reverseKeys(mapHG_parts._3)))
          }
      }
    }

    computeMBR(this.root)

    true
  }

  def findExact(searchXY: (Double, Double)): Point = {

    var currNode = this.root
    var checkX = true

    while (currNode != null) {

      val arr = currNode.lstPoints.filter(qtPoint => searchXY._1.equals(qtPoint.x) && searchXY._2.equals(qtPoint.y)).take(1)

      if (arr.nonEmpty)
        return arr.head
      else
        currNode match {
          case brn: KdtBranchRootNode =>
            if (if (checkX) searchXY._1 <= brn.lstPoints.head.x else searchXY._2 <= brn.lstPoints.head.y)
              currNode = brn.left
            else
              currNode = brn.right
          case _ =>
        }

      checkX = !checkX
    }

    null
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

    val sPtBestNodeInf = findBestNode(searchPoint, sortSetSqDist.maxSize)

    val dim = if (sortSetSqDist.isFull)
      math.sqrt(sortSetSqDist.last.distance)
    else
      computeDimension(searchPoint, sPtBestNodeInf._1.rectBounds)
    //        math.max(math.max(math.abs(searchPoint.x - sPtBestNodeInf.rectMBR.left), math.abs(searchPoint.x - sPtBestNodeInf.rectMBR.right)),
    //          math.max(math.abs(searchPoint.y - sPtBestNodeInf.rectMBR.bottom), math.abs(searchPoint.y - sPtBestNodeInf.rectMBR.top)))

    val searchRegion = Rectangle(searchPoint, new Geom2D(dim, dim))

    var prevMaxSqrDist = if (sortSetSqDist.last == null) -1 else sortSetSqDist.last.distance

    def process(kdtNode: KdtNode, splitX: Boolean, skipBranchRootNode: KdtNode) {

      val stackNode = mutable.Stack((kdtNode, splitX))

      while (stackNode.nonEmpty) {

        val (node, checkX) = stackNode.pop

        //        if (searchRegion.center match {
        //          case pt: Point => pt.userData.toString().equalsIgnoreCase("bread_3_a_822279")
        //          case _ => false
        //        })
        //          if (node.arrPoints.filter(_.userData.toString().equalsIgnoreCase("Bread_3_B_219256")).take(1).nonEmpty)
        //        println

        if (node != skipBranchRootNode)
          node match {
            case brn: KdtBranchRootNode =>

              if (testNode(brn, checkX, searchRegion)) {

                brn.lstPoints.foreach(pt => prevMaxSqrDist = testAndAddPoint(pt, searchRegion, sortSetSqDist, prevMaxSqrDist))

                if (brn.left != null)
                  stackNode.push((brn.left, !checkX))

                if (brn.right != null)
                  stackNode.push((brn.right, !checkX))
              }
            case ln: KdtNode =>
              ln.lstPoints.foreach(pt => prevMaxSqrDist = testAndAddPoint(pt, searchRegion, sortSetSqDist, prevMaxSqrDist))
          }
      }
    }

    process(sPtBestNodeInf._1, sPtBestNodeInf._2, null)

    if (sPtBestNodeInf._1 != this.root)
      process(this.root, splitX = true, sPtBestNodeInf._1)
  }

  def findBestNode(searchPoint: Geom2D, k: Int): (KdtNode, Boolean) = {

    // find leaf containing point
    var nodeCurr = root

    def testNode(kdtNode: KdtNode): Boolean =
      kdtNode != null && kdtNode.totalPoints >= k /*&& kdtNode.rectBounds.contains(searchPoint)*/

    var splitX = true

    while (true) {
      nodeCurr match {

        case brn: KdtBranchRootNode =>

          if (if (splitX) searchPoint.x <= brn.lstPoints.head.x else searchPoint.y <= brn.lstPoints.head.y) {
            if (testNode(brn.left))
              nodeCurr = brn.left
          }
          else if (testNode(brn.right))
            nodeCurr = brn.right
          else
            return (nodeCurr, splitX)

        case node: KdtNode =>
          return (node, splitX)
      }

      splitX = !splitX
    }

    null
  }

  //  iterPoints.map(pt => {
  //
  //    pointCount += 1
  //
  //    (math.floor(pt.x / barWidth), (math.floor(pt.y / barWidth), pt))
  //  })
  //    .toList
  //    .groupBy(_._1)
  //    .mapValues(_.map(_._2)
  //      .groupBy(_._1)
  //      .mapValues(_.map(_._2)))

  private def reverseKeys(mapHG: Map[Double, Map[Double, List[Point]]]): Map[Double, Map[Double, List[Point]]] =
    mapHG.map(keyMap => keyMap._2.map(keyList => (keyList._1, (keyMap._1, keyList._2))))
      .toList
      .flatMap(_.seq)
      .groupBy(_._1).map(xxx => xxx)
      .mapValues(_.map(_._2).map(xxx => xxx)
        .toMap)

  private def threeWayPartition(mapHG: Map[Double, Map[Double, List[Point]]], splitVal_key: Double): (Map[Double, Map[Double, List[Point]]], Map[Double, Map[Double, List[Point]]], Map[Double, Map[Double, List[Point]]]) = {

    val (mapLT_EQ, mapGT) = mapHG.partition(_._1 <= splitVal_key)
    val (mapEQ, mapLT) = mapLT_EQ.partition(_._1 == splitVal_key)

    (mapLT, mapEQ, mapGT)
    //      val mapLT = mutable.HashMap[Double, Map[Double, List[Point]]]()
    //      val mapEQ = mutable.HashMap[Double, Map[Double, List[Point]]]()
    //      val mapGT = mutable.HashMap[Double, Map[Double, List[Point]]]()
    //
    //      mapHG.foreach(row => {
    //        val map = row._1 match {
    //          case v if v < splitVal_key => mapLT
    //          case v if v == splitVal_key => mapEQ
    //          case v if v > splitVal_key => mapGT
    //        }
    //        map += (row._1 -> row._2)
    //      }
    //      )
    //
    //      (mapLT.toMap, mapEQ.toMap, mapGT.toMap)
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

  private def computeMBR(kdtNode: KdtNode): Rectangle = {

    kdtNode.rectBounds = buildRectBounds(computeBounds(kdtNode.lstPoints))

    kdtNode match {
      case brn: KdtBranchRootNode =>
        brn.rectBounds
          .mergeWith(if (brn.left == null) null else computeMBR(brn.left))
          .mergeWith(if (brn.right == null) null else computeMBR(brn.right))
      case _ =>
    }

    kdtNode.rectBounds
  }
}