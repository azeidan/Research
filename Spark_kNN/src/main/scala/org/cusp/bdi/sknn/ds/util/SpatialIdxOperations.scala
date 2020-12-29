package org.cusp.bdi.sknn.ds.util

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.SpatialIndex.maxSquaredEucDist
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.{KdTree, KdtBranchRootNode, KdtLeafNode, KdtNode}
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.sortset.{Node, SortedLinkedList}
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

final class GlobalIndexPointData extends KryoSerializable {

  var numPoints: Long = -1
  var partitionIdx: Int = -1

  def this(numPoints: Long, partitionIdx: Int) = {

    this()
    this.numPoints = numPoints
    this.partitionIdx = partitionIdx
  }

  override def equals(other: Any): Boolean = false

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeLong(numPoints)
    output.writeInt(partitionIdx)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    numPoints = input.readLong()
    partitionIdx = input.readInt()
  }
}

object SpatialIdxOperations extends Serializable {

  val SEARCH_REGION_EXTEND: Double = math.sqrt(8)

  final class IdxRangeLookupInfo {

    var rectSearchRegion: Rectangle = _
    val sortList: SortedLinkedList[Point] = new SortedLinkedList[Point]()
    var limitNode: Node[Point] = _
    var dimSquared: Double = 0
    var weight: Long = 0L

    def this(searchPoint: Geom2D, rectBestNode: Rectangle) = {

      this()

      rectSearchRegion = Rectangle(searchPoint, new Geom2D(math.sqrt(maxSquaredEucDist(searchPoint, rectBestNode)) + SEARCH_REGION_EXTEND))

      dimSquared = rectSearchRegion.halfXY.x * rectSearchRegion.halfXY.x
    }
  }

  def extractLstPartition(spatialIndex: SpatialIndex, searchXY: (Double, Double), k: Int): ListBuffer[Int] =
    (spatialIndex match {
      case quadTree: QuadTree => lookup(quadTree, searchXY, k)
      case kdTree: KdTree => lookup(kdTree, searchXY, k)
    })
      .map(_.data.userData match {
        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
      })
      .to[ListBuffer]
      .distinct

  private def lookup(quadTree: QuadTree, searchXY: (Double, Double), k: Int): SortedLinkedList[Point] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    val searchPoint = new Geom2D(searchXY._1, searchXY._2)

    val sPtBestQT = quadTree.findBestQuadrant(searchPoint, k)

    val idxRangeLookupInfo = new IdxRangeLookupInfo(searchPoint, sPtBestQT.rectBounds)

    def process(rootQT: QuadTree, skipQT: QuadTree) {

      val lstQT = ListBuffer(rootQT)

      lstQT.foreach(qt =>
        if (qt != skipQT)
          if (idxRangeLookupInfo.rectSearchRegion.intersects(qt.rectBounds)) {

            qt.lstPoints.foreach(updateMatchListAndRegion(_, idxRangeLookupInfo, k))

            if (qt.topLeft != null)
              lstQT += qt.topLeft
            if (qt.topRight != null)
              lstQT += qt.topRight
            if (qt.bottomLeft != null)
              lstQT += qt.bottomLeft
            if (qt.bottomRight != null)
              lstQT += qt.bottomRight
          })
    }

    process(sPtBestQT, null)

    if (sPtBestQT != quadTree)
      process(quadTree, sPtBestQT)

    idxRangeLookupInfo.sortList
  }

  private def lookup(kdTree: KdTree, searchXY: (Double, Double), k: Int): SortedLinkedList[Point] = {

    //            if (searchXY._1.toString().startsWith("248") && searchXY._2.toString().startsWith("58"))
    //              println

    val searchPoint = new Geom2D(searchXY._1, searchXY._2)

    var (sPtBestNode, splitX) = kdTree.findBestNode(searchPoint, k)

    val idxRangeLookupInfo = new IdxRangeLookupInfo(searchPoint, sPtBestNode.rectNodeBounds)

    def process(kdtNode: KdtNode, skipKdtNode: KdtNode) {

      val queueKdtNode = mutable.Queue((kdtNode, splitX))

      while (queueKdtNode.nonEmpty) {

        val row = queueKdtNode.dequeue()

        if (row._1 != skipKdtNode)
          row._1 match {
            case kdtBRN: KdtBranchRootNode =>
              if (kdtBRN.left != null && idxRangeLookupInfo.rectSearchRegion.intersects(kdtBRN.left.rectNodeBounds))
                queueKdtNode += ((kdtBRN.left, !row._2))
              if (kdtBRN.right != null && idxRangeLookupInfo.rectSearchRegion.intersects(kdtBRN.right.rectNodeBounds))
                queueKdtNode += ((kdtBRN.right, !row._2))

            case kdtLeafNode: KdtLeafNode =>
              if (idxRangeLookupInfo.rectSearchRegion.intersects(kdtLeafNode.rectNodeBounds))
                kdtLeafNode.lstPoints.foreach(updateMatchListAndRegion(_, idxRangeLookupInfo, k))
          }
      }
    }

    process(sPtBestNode, null)

    if (sPtBestNode != kdTree.rootNode) {

      splitX = true
      process(kdTree.rootNode, sPtBestNode)
    }

    idxRangeLookupInfo.sortList
  }

  private def updateMatchListAndRegion(point: Point, idxRangeLookupInfo: IdxRangeLookupInfo, k: Int): Unit = {

    //    if (point.x.toString().startsWith("143") && point.y.toString().startsWith("874"))
    //      print("")

    def getNumPoints(point: Point): Long = point.userData match {
      case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.numPoints
    }

    if (idxRangeLookupInfo.rectSearchRegion.contains(point)) {

      //              if (qtPoint.x.toString().startsWith("26157") && qtPoint.y.toString().startsWith("4965"))
      //                print("")

      val sqDistQTPoint = Helper.squaredEuclideanDist(idxRangeLookupInfo.rectSearchRegion.center.x, idxRangeLookupInfo.rectSearchRegion.center.y, point.x, point.y)

      // add point if it's within the search radius
      if (idxRangeLookupInfo.limitNode == null || sqDistQTPoint <= idxRangeLookupInfo.dimSquared) {

        idxRangeLookupInfo.sortList.add(sqDistQTPoint, point)

        idxRangeLookupInfo.weight += getNumPoints(point)

        // see if region can shrink and if at least the last node can be dropped
        if ((idxRangeLookupInfo.limitNode == null || idxRangeLookupInfo.sortList.last.data != point) &&
          (idxRangeLookupInfo.weight - getNumPoints(idxRangeLookupInfo.sortList.last.data)) >= k) {

          var elem = idxRangeLookupInfo.sortList.head
          var newWeight = getNumPoints(elem.data)

          while (newWeight < k) {

            elem = elem.next
            newWeight += getNumPoints(elem.data)
          }

          if (idxRangeLookupInfo.limitNode != elem) {

            idxRangeLookupInfo.limitNode = elem

            //            idxRangeLookupInfo.rectSearchRegion.halfXY.x = 2 + math.sqrt(idxRangeLookupInfo.limitNode.distance / 2)
            //            val maxManhattanDist = Helper.max(math.abs(idxRangeLookupInfo.rectSearchRegion.center.x - idxRangeLookupInfo.limitNode.data.x), math.abs(idxRangeLookupInfo.rectSearchRegion.center.y - idxRangeLookupInfo.limitNode.data.y))
            //            idxRangeLookupInfo.sqrDim = /*2 * */ math.pow(maxManhattanDist, 2) + 4 // +4 for the diagonal of an additional 2 squares (aka 2*sqrt(2)) to account for the floor operation of the grid assignment

            idxRangeLookupInfo.rectSearchRegion.halfXY.x = math.sqrt(idxRangeLookupInfo.limitNode.distance) + SEARCH_REGION_EXTEND
            idxRangeLookupInfo.rectSearchRegion.halfXY.y = idxRangeLookupInfo.rectSearchRegion.halfXY.x

            idxRangeLookupInfo.dimSquared = idxRangeLookupInfo.rectSearchRegion.halfXY.x * idxRangeLookupInfo.rectSearchRegion.halfXY.x

            while (elem.next != null && elem.next.distance <= idxRangeLookupInfo.dimSquared) {

              elem = elem.next
              newWeight += getNumPoints(elem.data)
            }

            idxRangeLookupInfo.sortList.stopAt(elem)
            idxRangeLookupInfo.weight = newWeight
          }
        }
      }
    }
  }
}