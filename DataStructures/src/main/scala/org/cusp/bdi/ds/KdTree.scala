package org.cusp.bdi.ds

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.KdTree.{buildRectBoundsFromNodePath, computeSplitKeyLoc, extractIterPoints, nodeCapacity}
import org.cusp.bdi.ds.SpatialIndex.{KnnLookupInfo, buildRectBounds, testAndAddPoint}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object KdTree extends Serializable {

  val nodeCapacity = 1

  def extractIterPoints(map: mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]]): Iterable[Point] =
    map.values.map(_.values).flatMap(_.seq).flatMap(_.seq)

  def buildRectBoundsFromNodePath(stackKdtNode: mutable.Stack[KdtNode]): Rectangle = {

    val rectBounds = new Rectangle(stackKdtNode.pop().rectPointBounds)

    while (stackKdtNode.nonEmpty)
      rectBounds.mergeWith(stackKdtNode.pop().rectPointBounds)

    rectBounds
  }

  def computeSplitKeyLoc(searchRegion: Rectangle, kdtBRN: KdtBranchRootNode, hgBarWidth: Int, splitX: Boolean): Int = {

    val limits = if (splitX)
      ((searchRegion.left / hgBarWidth).floor, (searchRegion.right / hgBarWidth).floor)
    else
      ((searchRegion.bottom / hgBarWidth).floor, (searchRegion.top / hgBarWidth).floor)

    kdtBRN.splitKey match {
      case sk if sk < limits._1 => 1
      case sk if sk > limits._2 => -1
      case _ => 0
    }
  }
}

class KdtNode extends KryoSerializable {

  var iterPoints: Iterable[Point] = _
  var totalPoints: Int = 0
  var rectPointBounds: Rectangle = _

  def this(iterPoints: Iterable[Point]) = {

    this()
    updateFields(iterPoints)
  }

  protected def updateFields(iterPoints: Iterable[Point]) {

    this.iterPoints = iterPoints
    this.totalPoints = iterPoints.size
    this.rectPointBounds = buildRectBounds(iterPoints)
  }

  override def toString: String =
    "%s\t%,d\t%,d".format(rectPointBounds, iterPoints.size, totalPoints)

  override def write(kryo: Kryo, output: Output): Unit = {

    kryo.writeClassAndObject(output, iterPoints)
    output.writeInt(totalPoints)
    kryo.writeClassAndObject(output, rectPointBounds)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    iterPoints = kryo.readClassAndObject(input).asInstanceOf[Iterable[Point]]
    totalPoints = input.readInt()
    rectPointBounds = kryo.readClassAndObject(input) match {
      case rectangle: Rectangle => rectangle
    }
  }
}

final class KdtBranchRootNode extends KdtNode {

  var splitKey: Double = 0
  var left: KdtNode = _
  var right: KdtNode = _

  def this(iterPoints: Iterable[Point], splitKey: Double) = {

    this()
    updateFields(iterPoints)
    this.splitKey = splitKey
  }

  override def toString: String = {

    val strR = if (right == null) '-' else '\\'
    val strL = if (left == null) '-' else '/'

    "%s\t%s %s".format(super.toString(), strL, strR)
  }

  override def write(kryo: Kryo, output: Output): Unit = {

    super.write(kryo, output)
    output.writeDouble(splitKey)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    super.read(kryo, input)
    splitKey = input.readDouble()
  }
}

class KdTree(hgBarWidth: Int) extends SpatialIndex {

  var root: KdtNode = _

  def getHGBarWidth: Int = hgBarWidth

  def getTotalPoints: Int = root.totalPoints

  override def insert(iterPoints: Iterator[Point]): Boolean = {

    if (root != null)
      throw new IllegalStateException("KD Tree already built")

    if (iterPoints.isEmpty)
      throw new IllegalStateException("Empty point iterator")

    if (hgBarWidth < 1)
      throw new IllegalStateException("Bar width must be >= 1")

    val mapHG = mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]]()

    iterPoints.foreach(pt => {
      //      if (pt.userData.toString.equalsIgnoreCase("Yellow_2_B_166836") ||
      //        pt.userData.toString.equalsIgnoreCase("Yellow_2_B_306524") ||
      //        pt.userData.toString.equalsIgnoreCase("Yellow_2_B_347320") ||
      //        pt.userData.toString.equalsIgnoreCase("Yellow_2_B_154991"))
      //        println

      val (keyX, keyY) = if (hgBarWidth == 1)
        (pt.x.floor, pt.y.floor)
      else
        ((pt.x / hgBarWidth).floor, (pt.y / hgBarWidth).floor)

      mapHG.getOrElseUpdate(keyX, mutable.HashMap[Double, ListBuffer[Point]]())
        .getOrElseUpdate(keyY, ListBuffer[Point]()) += pt
    })

    val lstNodeInfo = ListBuffer[(KdtBranchRootNode, Boolean, mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]], mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]])]()

    def buildNode(mapHG_node: mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]], splitX: Boolean) = {

      mapHG_node.size match {

        case 0 =>
          null
        case s if s <= nodeCapacity =>
          new KdtNode(extractIterPoints(mapHG_node))
        case _ =>

          val splitKeyMedian = {
            val arr = (if (splitX) mapHG_node.keySet else mapHG_node.valuesIterator.map(_.keySet).flatMap(_.seq))
              .to[mutable.SortedSet].toArray

            arr(arr.length / 2)
          }

          //          val splitKey = if (splitX)
          //            mapHG_node.mapValues(_.valuesIterator.map(_.size).sum).maxBy(_._2)._1 // mode
          //          else
          //            mapHG_node.valuesIterator.map(_.mapValues(_.size)).flatMap(_.seq).toList.groupBy(_._1).maxBy(_._2.size)._1 // mode

          //          val splitKey = if (splitX) {
          //
          //            val mapKeySum = mapHG_node.map(keyMap => (keyMap._1, keyMap._2.valuesIterator.map(_.size.toDouble).sum))
          //
          //            (mapKeySum.map(row => row._1 * row._2).sum / mapKeySum.valuesIterator.sum).floor
          //          }
          //          else {
          //            val mapKeySum = mapHG_node.valuesIterator.map(_.map(keyLst => (keyLst._1, keyLst._2.size.toDouble))).flatMap(_.seq).toList.groupBy(_._1).mapValues(_.map(_._2).sum)
          //
          //            (mapKeySum.map(row => row._1 * row._2).sum / mapKeySum.valuesIterator.sum).floor
          //          }

          val mapParts = partitionMap(mapHG_node, splitKeyMedian, splitX)

          if (mapParts._1.isEmpty && mapParts._3.isEmpty)
            new KdtNode(extractIterPoints(mapParts._2))
          else {

            val kdtBranchRootNode = new KdtBranchRootNode(extractIterPoints(mapParts._2), splitKeyMedian)

            lstNodeInfo += ((kdtBranchRootNode, splitX, mapParts._1, mapParts._3))

            kdtBranchRootNode
          }
      }
    }

    root = buildNode(mapHG, splitX = true)

    lstNodeInfo.foreach(row => {

      val (currNode, splitX, mapLeft, mapRight) = row

      currNode.left = buildNode(mapLeft, !splitX)
      currNode.right = buildNode(mapRight, !splitX)
    })

    this.root match {
      case kdtBRN: KdtBranchRootNode =>
        updateTotalPoint(kdtBRN)
      case _ =>
    }

    true
  }

  override def findExact(searchXY: (Double, Double)): Point = {

    var currNode = this.root

    val searchXY_grid = if (hgBarWidth == 1) (searchXY._1.floor, searchXY._2.floor) else ((searchXY._1 / hgBarWidth).floor, (searchXY._2 / hgBarWidth).floor)

    var splitX = true

    def findInIterPoints() = {

      val iter = currNode.iterPoints.filter(pt => pt.x.equals(searchXY._1) && pt.y.equals(searchXY._2)).take(1)

      if (iter.nonEmpty)
        iter.head
      else
        null
    }

    while (currNode != null)
      currNode match {
        case kdtBRN: KdtBranchRootNode =>

          (if (splitX) searchXY_grid._1 else searchXY_grid._2).compare(kdtBRN.splitKey).signum match {
            case -1 => currNode = kdtBRN.left
            case 0 => return findInIterPoints()
            case _ => currNode = kdtBRN.right
          }

          splitX = !splitX

        case _ =>
          return findInIterPoints()
      }

    null
  }

  def findBestNode(searchPoint: Geom2D, k: Int): (mutable.Stack[KdtNode], Boolean) = {

    // find leaf containing point
    var currNode = root
    val searchXY_grid = if (hgBarWidth == 1) (searchPoint.x, searchPoint.y) else (searchPoint.x / hgBarWidth, searchPoint.y / hgBarWidth)
    var splitX = true

    val stackNodePath = mutable.Stack[KdtNode]()

    while (true) {

      stackNodePath.push(currNode)

      currNode match {
        case kdtBRN: KdtBranchRootNode =>

          (if (splitX) searchXY_grid._1 else searchXY_grid._2).compare(kdtBRN.splitKey).signum match {
            case 1 =>
              if (kdtBRN.right != null && kdtBRN.right.totalPoints >= k /*&& kdtBRN.right.rectPointBounds.contains(searchPoint)*/ )
                currNode = kdtBRN.right
              else
                return (stackNodePath, splitX)
            //            case 0 =>
            //              return (stackNodePath, splitX)
            case _ =>
              if (kdtBRN.left != null && kdtBRN.left.totalPoints >= k /*&& kdtBRN.left.rectPointBounds.contains(searchPoint)*/ )
                currNode = kdtBRN.left
              else
                return (stackNodePath, splitX)
          }
          splitX = !splitX

        case _: KdtNode =>
          return (stackNodePath, splitX)
      }
    }

    null
  }

  //  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point]) {
  //
  //    val sPtBestNodeInfo = findBestNode(searchPoint, sortSetSqDist.maxSize)
  //    val sPtBestNode = sPtBestNodeInfo._1.top
  //    //    var splitX = sPtBestNodeInfo._2
  //
  //    val dim = if (sortSetSqDist.isFull)
  //      math.sqrt(sortSetSqDist.last.distance)
  //    else
  //      computeDimension(searchPoint, buildRectBoundsFromNodePath(sPtBestNodeInfo._1))
  //
  //    val searchRegion = Rectangle(searchPoint, new Geom2D(dim))
  //
  //    var prevMaxSqrDist = if (sortSetSqDist.last == null) -1 else sortSetSqDist.last.distance
  //    var skipKdtNode: KdtNode = null
  //
  //    def process(kdtNode: KdtNode, splitX: Boolean) {
  //
  //      if (kdtNode != skipKdtNode) {
  //
  //        kdtNode match {
  //          case kdtBRN: KdtBranchRootNode =>
  //
  //            computeSplitKeyLoc(searchRegion, kdtBRN, hgBarWidth, splitX) match {
  //              case -1 =>
  //                if (kdtBRN.left != null)
  //                  process(kdtBRN.left, !splitX)
  //              case 1 =>
  //                if (kdtBRN.right != null)
  //                  process(kdtBRN.right, !splitX)
  //              case _ =>
  //                if (kdtBRN.left != null)
  //                  process(kdtBRN.left, !splitX)
  //                if (kdtBRN.right != null)
  //                  process(kdtBRN.right, !splitX)
  //            }
  //
  //          case _ =>
  //        }
  //
  //        if (searchRegion.intersects(kdtNode.rectPointBounds))
  //          kdtNode.iterPoints.foreach(pt => prevMaxSqrDist = testAndAddPoint(pt, searchRegion, sortSetSqDist, prevMaxSqrDist))
  //      }
  //    }
  //
  //    process(sPtBestNode, sPtBestNodeInfo._2)
  //
  //    if (sPtBestNode != this.root) {
  //
  //      skipKdtNode = sPtBestNode
  //      process(this.root, true)
  //    }
  //  }

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point]) {

    //    if (searchPoint.userData.toString.equalsIgnoreCase("yellow_2_a_776229"))
    //      println

    val sPtBestNodeInfo = findBestNode(searchPoint, sortSetSqDist.maxSize)
    val sPtBestNode = sPtBestNodeInfo._1.top
    var splitX = sPtBestNodeInfo._2

    //    val dim = if (sortSetSqDist.isFull)
    //      math.sqrt(sortSetSqDist.last.distance)
    //    else
    //      computeDimension(searchPoint, buildRectBoundsFromNodePath(sPtBestNodeInfo._1))
    //
    //    val searchRegion = Rectangle(searchPoint, new Geom2D(dim))
    //
    //    var prevMaxSqrDist = if (sortSetSqDist.last == null) -1 else sortSetSqDist.last.distance

    val knnLookupInfo = KnnLookupInfo(searchPoint, sortSetSqDist, buildRectBoundsFromNodePath(sPtBestNodeInfo._1))

    def process(kdtNode: KdtNode, skipKdtNode: KdtNode) {

      val stackKdtNode = mutable.Stack((kdtNode, splitX))

      // depth-first search
      while (stackKdtNode.nonEmpty) {

        val (kdtNode, splitX) = stackKdtNode.pop()

        if (kdtNode != skipKdtNode) {

          if (knnLookupInfo.rectSearchRegion.intersects(kdtNode.rectPointBounds))
            kdtNode.iterPoints.foreach(testAndAddPoint(_, knnLookupInfo))

          kdtNode match {
            case kdtBRN: KdtBranchRootNode =>

              computeSplitKeyLoc(knnLookupInfo.rectSearchRegion, kdtBRN, hgBarWidth, splitX) match {
                case -1 =>
                  if (kdtBRN.left != null)
                    stackKdtNode.push((kdtBRN.left, !splitX))
                case 1 =>
                  if (kdtBRN.right != null)
                    stackKdtNode.push((kdtBRN.right, !splitX))
                case _ =>
                  if (kdtBRN.left != null)
                    stackKdtNode.push((kdtBRN.left, !splitX))
                  if (kdtBRN.right != null)
                    stackKdtNode.push((kdtBRN.right, !splitX))
              }

            case _ =>
          }
        }
      }
    }

    process(sPtBestNode, null)

    if (sPtBestNode != this.root) {

      splitX = true
      process(this.root, sPtBestNode)
    }
  }

  private def partitionMap(mapHG_node: mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]], splitVal_key: Double, splitX: Boolean) = {

    val mapLT = mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]]()
    val mapEQ = mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]]()
    val mapGT = mutable.HashMap[Double, mutable.HashMap[Double, ListBuffer[Point]]]()

    if (splitX)
      mapHG_node.foreach(keyMap =>
        (keyMap._1.compare(splitVal_key).signum match {
          case -1 => mapLT
          case 0 => mapEQ
          case _ => mapGT
        })
          += keyMap
      )
    else
      mapHG_node.foreach(keyMap => {

        val mapLT_Y = mutable.HashMap[Double, ListBuffer[Point]]()
        val mapEQ_Y = mutable.HashMap[Double, ListBuffer[Point]]()
        val mapGT_Y = mutable.HashMap[Double, ListBuffer[Point]]()

        keyMap._2.foreach(keyLst =>
          (keyLst._1.compare(splitVal_key).signum match {
            case -1 => mapLT_Y
            case 0 => mapEQ_Y
            case _ => mapGT_Y
          })
            += keyLst
        )

        if (mapLT_Y.nonEmpty)
          mapLT += ((keyMap._1, mapLT_Y))
        if (mapEQ_Y.nonEmpty)
          mapEQ += ((keyMap._1, mapEQ_Y))
        if (mapGT_Y.nonEmpty)
          mapGT += ((keyMap._1, mapGT_Y))
      })

    (mapLT, mapEQ, mapGT)
  }

  override def toString: String =
    "%s".format(root)

  override def write(kryo: Kryo, output: Output): Unit = {

    val lstNode = ListBuffer(this.root)

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

    this.root = readNode()

    val lstNode = ListBuffer(this.root)

    lstNode.foreach {
      case brn: KdtBranchRootNode =>
        brn.left = readNode()
        brn.right = readNode()

        lstNode += (brn.left, brn.right)
      case _ =>
    }
  }

  private def updateTotalPoint(kdtBRN: KdtBranchRootNode) {

    if (kdtBRN.left != null) {
      kdtBRN.left match {
        case kdtBRN_child: KdtBranchRootNode =>
          updateTotalPoint(kdtBRN_child)
        case _ =>
      }

      kdtBRN.totalPoints += kdtBRN.left.totalPoints
    }

    if (kdtBRN.right != null) {
      kdtBRN.right match {
        case kdtBRN_child: KdtBranchRootNode =>
          updateTotalPoint(kdtBRN_child)
        case _ =>
      }

      kdtBRN.totalPoints += kdtBRN.right.totalPoints
    }
  }
}