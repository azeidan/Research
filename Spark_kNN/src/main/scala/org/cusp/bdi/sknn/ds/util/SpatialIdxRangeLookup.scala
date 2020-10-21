package org.cusp.bdi.sknn.ds.util

import org.cusp.bdi.ds.KdTree.{buildRectBoundsFromNodePath, computeSplitKeyLoc}
import org.cusp.bdi.ds.SpatialIndex.computeDimension
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.sknn.GlobalIndexPointData
import org.cusp.bdi.util.Helper

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait PointData extends Serializable {

  def numPoints: Int

  def equals(other: Any): Boolean
}

object SpatialIdxRangeLookup extends Serializable {

  val errorRange: Float = math.sqrt(8).toFloat

  case class IdxRangeLookupInfo(rectSearchRegion: Rectangle) {

    val sortList: SortedList[Point] = new SortedList[Point]()
    var limitNode: Node[Point] = _
    var sqDim: Double = math.pow(rectSearchRegion.halfXY.x, 2)
    var weight: Long = 0L
  }

  def getLstPartition(spatialIndex: SpatialIndex, searchXY: (Double, Double), k: Int): List[Int] =
    (spatialIndex match {
      case quadTree: QuadTree => lookup(quadTree, searchXY, k)
      case kdTree: KdTree => lookup(kdTree, searchXY, k)
    })
      .map(_.data.userData match {
        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
      })
      .toSet
      .toList

  private def lookup(quadTree: QuadTree, searchXY: (Double, Double), k: Int): SortedList[Point] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    val searchPoint = new Geom2D(searchXY._1, searchXY._2)

    val sPtBestQT = quadTree.findBestQuadrant(searchPoint, k)

    val idxRangeLookupInfo = IdxRangeLookupInfo(Rectangle(searchPoint, new Geom2D(computeDimension(searchPoint, sPtBestQT.rectBounds))))

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

  private def lookup(kdTree: KdTree, searchXY: (Double, Double), k: Int): SortedList[Point] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    val searchPoint = new Geom2D(searchXY._1, searchXY._2)

    val sPtBestNodeInfo = kdTree.findBestNode(searchPoint, k)
    val sPtBestNode = sPtBestNodeInfo._1.top
    var splitX = sPtBestNodeInfo._2

    val idxRangeLookupInfo = IdxRangeLookupInfo(Rectangle(searchPoint, new Geom2D(computeDimension(searchPoint, buildRectBoundsFromNodePath(sPtBestNodeInfo._1)))))

    def process(kdtNode: KdtNode, skipKdtNode: KdtNode) {

      val stackKdtNode = mutable.Stack((kdtNode, splitX))

      while (stackKdtNode.nonEmpty) {

        val (kdtNode, splitX) = stackKdtNode.pop()

        if (kdtNode != skipKdtNode) {

          if (idxRangeLookupInfo.rectSearchRegion.intersects(kdtNode.rectPointBounds))
            kdtNode.iterPoints.foreach(updateMatchListAndRegion(_, idxRangeLookupInfo, k))

          kdtNode match {
            case kdtBRN: KdtBranchRootNode =>

              computeSplitKeyLoc(idxRangeLookupInfo.rectSearchRegion, kdtBRN, kdTree.getHGBarWidth, splitX) match {
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

    if (sPtBestNode != kdTree.root) {

      splitX = true
      process(kdTree.root, sPtBestNode)
    }

    idxRangeLookupInfo.sortList
  }

  //  private def buildIdxRangeLookupInfo(searchPoint: Geom2D, rectMBR: Rectangle): IdxRangeLookupInfo =
  //    IdxRangeLookupInfo(Rectangle(searchPoint, new Geom2D(computeDimension(searchPoint, rectMBR))))

  private def updateMatchListAndRegion(point: Point, idxRangeLookupInfo: IdxRangeLookupInfo, k: Int): Unit = {

    def getNumPoints(point: Point): Long = point.userData match {
      case pointData: PointData => pointData.numPoints
      case _ => throw new ClassCastException("Point.userdata must extend " + classOf[PointData].getName)
    }

    if (idxRangeLookupInfo.rectSearchRegion.contains(point)) {

      //              if (qtPoint.x.toString().startsWith("26157") && qtPoint.y.toString().startsWith("4965"))
      //                print("")

      val sqDistQTPoint = Helper.squaredDist(idxRangeLookupInfo.rectSearchRegion.center.x, idxRangeLookupInfo.rectSearchRegion.center.y, point.x, point.y)

      // add point if it's within the search radius
      if (idxRangeLookupInfo.limitNode == null || sqDistQTPoint < idxRangeLookupInfo.sqDim) {

        idxRangeLookupInfo.sortList.add(sqDistQTPoint, point)

        idxRangeLookupInfo.weight += getNumPoints(point)

        // see if region can shrink if at least the last node can be dropped
        if ((idxRangeLookupInfo.limitNode == null || idxRangeLookupInfo.sortList.last.data != point) && (idxRangeLookupInfo.weight - getNumPoints(idxRangeLookupInfo.sortList.last.data)) >= k) {

          var elem = idxRangeLookupInfo.sortList.head
          var newWeight = getNumPoints(elem.data)

          while (newWeight < k) {

            elem = elem.next
            newWeight += getNumPoints(elem.data)
          }

          if (idxRangeLookupInfo.limitNode != elem) {

            idxRangeLookupInfo.limitNode = elem

            idxRangeLookupInfo.rectSearchRegion.halfXY.x = math.sqrt(idxRangeLookupInfo.limitNode.distance) + errorRange
            idxRangeLookupInfo.rectSearchRegion.halfXY.y = idxRangeLookupInfo.rectSearchRegion.halfXY.x

            idxRangeLookupInfo.sqDim = math.pow(idxRangeLookupInfo.rectSearchRegion.halfXY.x, 2)

            while (elem.next != null && elem.next.distance < idxRangeLookupInfo.sqDim) {

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