package org.cusp.bdi.ds

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.KdTree.{computeRectMBR, nodeCapacity}
import org.cusp.bdi.ds.SpatialIndex.{testAndAddPoint, updateMatchListAndRegion}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
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

  var arrPoints: Array[Point] = _arrPoints
  var totalPoints: Int = if (_arrPoints == null) 0 else _arrPoints.length
  var rectMBR: Rectangle = if (_arrPoints == null) null else computeRectMBR(_arrPoints)

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

final class KdtBranchRootNode(arrPoints: Array[Point]) extends KdtNode(null) {

  var left: KdtNode = _
  var right: KdtNode = _

  totalPoints = arrPoints.length

  override def toString: String = {

    val strR = if (left == null) 'X' else '\\'
    val strL = if (left == null) 'X' else '/'

    "%s\t%s\t%s".format(super.toString, strL, strR)
  }

  //  override def write(kryo: Kryo, output: Output): Unit = {
  //
  //    super.write(kryo, output)
  //  }
  //
  //  override def read(kryo: Kryo, input: Input): Unit = {
  //
  //    super.read(kryo, input)
  //  }
}

object TestKdTree {

  def main(args: Array[String]): Unit = {

    val lstPoint = ListBuffer(new Point(0.54, 0.93), new Point(0.96, 0.86), new Point(0.42, 0.67), new Point(0.11, 0.53), new Point(0.64, 0.29), new Point(0.27, 0.75), new Point(0.81, 0.63))

    val kdt = new KdTree()
    kdt.insert(lstPoint.iterator)
    kdt.printInOrder()

    println(kdt.findExact((9, 1)))
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

        currNode.arrPoints = arrSplitLists(0)
        currNode.rectMBR = computeRectMBR(arrSplitLists(0))

        if (arrSplitLists(1).nonEmpty)
          if (arrSplitLists(1).length <= nodeCapacity)
            currNode.left = new KdtNode(arrSplitLists(1))
          else {

            kdtBranchRootNode = new KdtBranchRootNode(arrSplitLists(1))
            currNode.left = kdtBranchRootNode
            queueProcess += ((kdtBranchRootNode, !splitX, arrSplitLists(1)))
          }

        if (arrSplitLists(2).nonEmpty)
          if (arrSplitLists(2).length <= nodeCapacity)
            currNode.right = new KdtNode(arrSplitLists(2))
          else {

            kdtBranchRootNode = new KdtBranchRootNode(arrSplitLists(2))
            currNode.right = kdtBranchRootNode
            queueProcess += ((kdtBranchRootNode, !splitX, arrSplitLists(2)))
          }
      }
    }

    // update MBRs
    updateMBR(this.root)

    def updateMBR(kdtNode: KdtNode): Rectangle =
      kdtNode match {
        case brn: KdtBranchRootNode =>
          brn.rectMBR
            .mergeWith(if (brn.left == null) null else updateMBR(brn.left))
            .mergeWith(if (brn.right == null) null else updateMBR(brn.right))

          brn.rectMBR
        case node: KdtNode =>
          node.rectMBR
        //        case _ => null
      }

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

  def printInOrder(node: KdtNode, delimiter: String): Unit = {

    if (node != null) {

      node match {
        case brn: KdtBranchRootNode =>
          printInOrder(brn.left, delimiter + "\t")
          printInOrder(brn.right, delimiter + "\t")
        case node: KdtNode =>
          println("%s%s".format(delimiter, node))
      }
    }
  }

  override def toString: String =
    "%s".format(root)

  override def write(kryo: Kryo, output: Output): Unit = {

    //    println(">>> kdt serialization start " + root)

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

    //    println(">>> kdt serialization done " + root)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    //    println(">>> kdt de-serialization start " + root)

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

    //    println(">>> kdt de-serialization end " + root)
  }

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int) {

    //    if (searchPoint.userData.toString().equalsIgnoreCase("taxi_1_a_298697"))
    //      println

    var searchRegion: Rectangle = null
    var sPtBestNode: KdtNode = null

    sPtBestNode = getBestNode(searchPoint, k)

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

    val prevMaxSqrDist = new DoubleWrapper(if (sortSetSqDist.last == null) -1 else sortSetSqDist.last.distance)

    def process(kdtNode: KdtNode, skipBranchRootNode: KdtNode) {

      val stackNode = mutable.Stack(kdtNode)

      while (stackNode.nonEmpty) {

        val node = stackNode.pop

        if (node != skipBranchRootNode && searchRegion.intersects(node.rectMBR)) {

          node.arrPoints.foreach(testAndAddPoint(_, searchRegion, sortSetSqDist, prevMaxSqrDist))

          node match {
            case brn: KdtBranchRootNode =>

              if (brn.left != null && brn.left.rectMBR.intersects(searchRegion))
                stackNode.push(brn.left)

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

  override def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int): SortedList[Point] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    val searchPoint = new Geom2D(searchXY._1, searchXY._2)

    val sPtBestNode = getBestNode(searchPoint, k)

    val dim = math.max(math.max(math.abs(searchPoint.x - sPtBestNode.rectMBR.left), math.abs(searchPoint.x - sPtBestNode.rectMBR.right)),
      math.max(math.abs(searchPoint.y - sPtBestNode.rectMBR.bottom), math.abs(searchPoint.y - sPtBestNode.rectMBR.top)))

    val searchRegion = Rectangle(searchPoint, new Geom2D(dim, dim))

    spatialIdxRangeLookupHelper(sPtBestNode, searchRegion, k)
    //      .map(_.data.userData match {
    //        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
    //      })
    //      .toSet
  }

  private def spatialIdxRangeLookupHelper(sPtBestNode: KdtNode, searchRegion: Rectangle, k: Int) = {

    val sortList = SortedList[Point](Int.MaxValue)
    val searchRegionInfo = new SearchRegionInfo(sortList.head, math.pow(searchRegion.halfXY.x, 2))

    def process(branchRootNode: KdtNode, skipBranchRootNode: KdtNode) {

      val stackNode = mutable.Stack(branchRootNode)

      while (stackNode.nonEmpty) {

        val node = stackNode.pop

        if (node != skipBranchRootNode && searchRegion.intersects(node.rectMBR)) {

          node.arrPoints.foreach(updateMatchListAndRegion(_, searchRegion, sortList, k, searchRegionInfo))

          node match {
            case brn: KdtBranchRootNode =>

              if (brn.left != null && brn.left.rectMBR.intersects(searchRegion))
                stackNode.push(brn.left)

              if (brn.right != null && brn.right.rectMBR.intersects(searchRegion))
                stackNode.push(brn.right)

            case _ =>
          }
        }
      }
    }

    process(sPtBestNode, null)

    if (sPtBestNode != this.root)
      process(this.root, sPtBestNode)

    sortList
  }

  private def getBestNode(searchPoint: Geom2D, k: Int): KdtNode = {

    // find leaf containing point
    var nodeCurr = root

    def testNode(kdtNode: KdtNode) =
      kdtNode != null && kdtNode.totalPoints >= k && kdtNode.rectMBR.contains(searchPoint)

    while (true)
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

    null
  }
}