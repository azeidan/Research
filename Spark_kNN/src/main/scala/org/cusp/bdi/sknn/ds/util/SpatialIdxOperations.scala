package org.cusp.bdi.sknn.ds.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.{KdTree, KdtBranchRootNode, KdtLeafNode, KdtNode}
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.sortset.{Node, SortedLinkedList}
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object SupportedSpatialIndexes extends Enumeration with Serializable {

  val quadTree: SupportedSpatialIndexes.Value = Value("qt")
  val kdTree: SupportedSpatialIndexes.Value = Value("kdt")

  def apply(spatialIndexType: SupportedSpatialIndexes.Value): SpatialIndex =
    spatialIndexType match {
      case SupportedSpatialIndexes.quadTree => new QuadTree()
      case SupportedSpatialIndexes.kdTree => new KdTree()
      case _ => throw new IllegalArgumentException("Unsupported Spatial Index Type: " + spatialIndexType)
    }
}

final class GlobalIndexPointUserData extends KryoSerializable {

  var objCount: Long = -1
  var partitionIdx: Int = -1

  def this(numPoints: Long, partIdx: Int) = {
    this()
    this.objCount = numPoints
    this.partitionIdx = partIdx
  }

  //  def this(numPoints: Long) = {
  //    this()
  //    this.objCount = numPoints
  //  }

  //  override def equals(other: Any): Boolean =false
  //    other match {
  //      case globalIndexPointUserData: GlobalIndexPointUserData => globalIndexPointUserData.objCount == this.objCount && globalIndexPointUserData.partitionIdx == this.partitionIdx
  //      case _ => false
  //    }

  override def toString: String =
    "%,d %,d".format(objCount, partitionIdx)

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeLong(objCount)
    output.writeInt(partitionIdx)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    objCount = input.readLong()
    partitionIdx = input.readInt()
  }
}

object SpatialIdxOperations extends Serializable {

  // best case is the two points are d units apart where d is a multiple of sqrt(2)
  // To account for the expansion of one square, add sqrt(2). The other sqrt(2)-1 is to account
  //  for the shift from the best case (multiple of 2) to a 1 (i.e. the point is d` units away where d` is a multiple of 1)
  val SEARCH_REGION_EXTEND: Double = 2 * Math.sqrt(2) - 1
  //  val SQRT_2: Double = Math.sqrt(2)
  //  val SQRT_2_LESS_ONE: Double = Math.sqrt(2) - 1

  def fCastToGlobalIndexPointUserData: Point => GlobalIndexPointUserData = (point: Point) => point.userData match {
    case globalIndexPoint: GlobalIndexPointUserData => globalIndexPoint
  }

  final class IdxRangeLookupInfo {

    var rectSearchRegion: Rectangle = _
    val sortList: SortedLinkedList[Point] = new SortedLinkedList[Point]()
    var limitNode: Node[Point] = _
    var dimSquared: Double = Double.MaxValue
    var weight: Long = 0L

    def this(searchPoint: Geom2D) {

      this()
      this.rectSearchRegion = Rectangle(searchPoint, new Geom2D(Double.MaxValue))
    }
  }

  def extractLstPartition(spatialIndex: SpatialIndex, searchXY: (Int, Int), k: Int): ArrayBuffer[Int] =
    (spatialIndex match {
      case quadTree: QuadTree => lookup(quadTree, searchXY, k)
      case kdTree: KdTree => lookup(kdTree, searchXY, k)
    })
      .map(point => fCastToGlobalIndexPointUserData(point.data).partitionIdx)
      .to[ArrayBuffer]
      .distinct

  private def lookup(quadTree: QuadTree, searchXY: (Int, Int), k: Int): SortedLinkedList[Point] = {

    //    if (searchXY._1 == 19922 && searchXY._2 == 3940 /*&& point.x == 19917 && point.y == 4399*/ )
    //      println

    val searchPoint = new Geom2D(searchXY._1, searchXY._2)

    val idxRangeLookupInfo = new IdxRangeLookupInfo(searchPoint)

    def process(rootQT: QuadTree, skipQT: QuadTree) {

      val qQT = mutable.Queue(rootQT)

      while (qQT.nonEmpty) {

        val qt = qQT.dequeue()

        if (qt != skipQT)
          if (idxRangeLookupInfo.rectSearchRegion.intersects(qt.rectBounds)) {

            qt.arrPoints.foreach(point =>
              if (idxRangeLookupInfo.rectSearchRegion.contains(point))
                updateMatchListAndRegion(point, idxRangeLookupInfo, k))

            if (qt.topLeft != null)
              qQT += qt.topLeft
            if (qt.topRight != null)
              qQT += qt.topRight
            if (qt.bottomLeft != null)
              qQT += qt.bottomLeft
            if (qt.bottomRight != null)
              qQT += qt.bottomRight
          }
      }
    }

    val sPtBestQT = quadTree.findBestQuadrant(searchPoint, k)

    process(sPtBestQT, null)

    if (sPtBestQT != quadTree)
      process(quadTree, sPtBestQT)

    //    if (idxRangeLookupInfo.sortList.map(_.data.userData.asInstanceOf[GlobalIndexPointUserData].partitionIdx).toArray.distinct.length > 25)
    //      Helper.loggerSLf4J(true, SparkKnn, ">>>\t%s\t%s".format(searchPoint.toString(), idxRangeLookupInfo.sortList.mkString(",\t")), null)

    idxRangeLookupInfo.sortList
  }

  private def lookup(kdTree: KdTree, searchXY: (Int, Int), k: Int): SortedLinkedList[Point] = {

    //            if (searchXY._1.toString().startsWith("248") && searchXY._2.toString().startsWith("58"))
    //              println

    val searchPoint = new Geom2D(searchXY._1, searchXY._2)

    val idxRangeLookupInfo = new IdxRangeLookupInfo(searchPoint)

    def process(kdtNode: KdtNode, skipKdtNode: KdtNode) {

      val qKdtNode = mutable.Queue(kdtNode)

      while (qKdtNode.nonEmpty) {

        val node = qKdtNode.dequeue()

        if (node != skipKdtNode)
          node match {
            case kdtBRN: KdtBranchRootNode =>
              if (kdtBRN.left != null && idxRangeLookupInfo.rectSearchRegion.intersects(kdtBRN.left.rectNodeBounds))
                qKdtNode += kdtBRN.left
              if (kdtBRN.right != null && idxRangeLookupInfo.rectSearchRegion.intersects(kdtBRN.right.rectNodeBounds))
                qKdtNode += kdtBRN.right

            case kdtLeafNode: KdtLeafNode =>
              if (idxRangeLookupInfo.rectSearchRegion.intersects(kdtLeafNode.rectNodeBounds))
                kdtLeafNode.arrPoints
                  .filter(idxRangeLookupInfo.rectSearchRegion.contains)
                  .foreach(point =>
                    updateMatchListAndRegion(point, idxRangeLookupInfo, k))
          }
      }
    }

    val sPtBestNode = kdTree.findBestNode(searchPoint, k)

    process(sPtBestNode, null)

    if (sPtBestNode != kdTree.rootNode)
      process(kdTree.rootNode, sPtBestNode)

    idxRangeLookupInfo.sortList
  }

  private def updateMatchListAndRegion(point: Point, idxRangeLookupInfo: IdxRangeLookupInfo, k: Int): Unit = {

    //    if (idxRangeLookupInfo.rectSearchRegion.center.x == 19693&& idxRangeLookupInfo.rectSearchRegion.center.y == 2994 /*&& point.x == 19917 && point.y == 4399*/ )
    //      print("")
    //
    //    if (point.x == 19690 && point.y == 2992)
    //      println

    def updateSearchRegion() {

      //      val extendX = idxRangeLookupInfo.limitNode.data.x + (if (idxRangeLookupInfo.limitNode.data.x > idxRangeLookupInfo.rectSearchRegion.center.x) 1 else -1)
      //      val extendY = idxRangeLookupInfo.limitNode.data.y + (if (idxRangeLookupInfo.limitNode.data.y > idxRangeLookupInfo.rectSearchRegion.center.y) 1 else -1)

      val dist = Math.sqrt(idxRangeLookupInfo.limitNode.distance) + SEARCH_REGION_EXTEND

      idxRangeLookupInfo.rectSearchRegion.halfXY.x = dist.floor
      idxRangeLookupInfo.rectSearchRegion.halfXY.y = idxRangeLookupInfo.rectSearchRegion.halfXY.x
      idxRangeLookupInfo.dimSquared = dist * dist // Helper.squaredEuclideanDist(idxRangeLookupInfo.rectSearchRegion.center.x, idxRangeLookupInfo.rectSearchRegion.center.y, extendX, extendY)
      //
      //      idxRangeLookupInfo.dimSquared += 1e-4
    }

    val sqDistQTPoint = Helper.squaredEuclideanDist(idxRangeLookupInfo.rectSearchRegion.center.x, idxRangeLookupInfo.rectSearchRegion.center.y, point.x, point.y)

    // add point if it's within the search radius
    if (sqDistQTPoint <= idxRangeLookupInfo.dimSquared) {

      idxRangeLookupInfo.sortList.add(sqDistQTPoint, point)

      idxRangeLookupInfo.weight += fCastToGlobalIndexPointUserData(point).objCount

      if (idxRangeLookupInfo.weight >= k)
        if (idxRangeLookupInfo.limitNode eq null) {

          idxRangeLookupInfo.limitNode = idxRangeLookupInfo.sortList.last

          updateSearchRegion()
        }
        else if (sqDistQTPoint < idxRangeLookupInfo.limitNode.distance) {

          var currNode = idxRangeLookupInfo.sortList.head
          var newWeight = fCastToGlobalIndexPointUserData(currNode.data).objCount
          var nodeCount = 1

          while (newWeight < k) {

            currNode = currNode.next
            nodeCount += 1
            newWeight += fCastToGlobalIndexPointUserData(currNode.data).objCount
          }

          if ((idxRangeLookupInfo.limitNode ne currNode) && idxRangeLookupInfo.limitNode.distance != currNode.distance) {

            idxRangeLookupInfo.limitNode = currNode
            idxRangeLookupInfo.weight = newWeight

            updateSearchRegion()

            while (currNode.next != null && currNode.next.distance <= idxRangeLookupInfo.dimSquared) {

              currNode = currNode.next
              idxRangeLookupInfo.weight += fCastToGlobalIndexPointUserData(currNode.data).objCount
              nodeCount += 1
            }

            idxRangeLookupInfo.sortList.stopAt(currNode, nodeCount)
          }
        }
    }
  }
}