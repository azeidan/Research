package org.cusp.bdi.sknn.ds.util

import org.cusp.bdi.ds.SpatialIndex.computeDimension
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.KdTree.findSearchRegionLocation
import org.cusp.bdi.ds.kdt.{KdTree, KdtBranchRootNode, KdtLeafNode, KdtNode}
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.sortset.{Node, SortedList}
import org.cusp.bdi.sknn.GlobalIndexPointData
import org.cusp.bdi.util.Helper

import scala.collection.mutable.ListBuffer

trait PointData extends Serializable {

  def numPoints: Int

  def equals(other: Any): Boolean
}

object SpatialIdxRangeLookup extends Serializable {

  val errorRange: Float = math.sqrt(8).toFloat

  class IdxRangeLookupInfo {

    var rectSearchRegion: Rectangle = _
    val sortList: SortedList[Point] = new SortedList[Point]()
    var limitNode: Node[Point] = _
    var sqrDim: Double = 0
    var weight: Long = 0L

    def this(searchPoint: Geom2D, rectBestNode: Rectangle) = {

      this()

      rectSearchRegion = Rectangle(searchPoint, new Geom2D(computeDimension(searchPoint, rectBestNode)))
      sqrDim = math.pow(rectSearchRegion.halfXY.x, 2)
    }
  }

  def getLstPartition(spatialIndex: SpatialIndex, searchXY: (Double, Double), k: Int): ListBuffer[Int] =
    (spatialIndex match {
      case quadTree: QuadTree => lookup(quadTree, searchXY, k)
      case kdTree: KdTree => lookup(kdTree, searchXY, k)
    })
      .map(_.data.userData match {
        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
      })
      .toSet
      .to[ListBuffer]

  private def lookup(quadTree: QuadTree, searchXY: (Double, Double), k: Int): SortedList[Point] = {

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

  private def lookup(kdTree: KdTree, searchXY: (Double, Double), k: Int): SortedList[Point] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    val searchPoint = new Geom2D(searchXY._1, searchXY._2)

    var (sPtBestNode, splitX) = kdTree.findBestNode(searchPoint, k)

    val idxRangeLookupInfo = new IdxRangeLookupInfo(searchPoint, sPtBestNode.rectNodeBounds)

    def process(kdtNode: KdtNode, skipKdtNode: KdtNode) {

      val lstKdtNode = ListBuffer((kdtNode, splitX))

      lstKdtNode.foreach(row =>
        if (row._1 != skipKdtNode)
          row._1 match {
            case kdtBRN: KdtBranchRootNode =>

              findSearchRegionLocation(idxRangeLookupInfo.rectSearchRegion, kdtBRN.splitVal, row._2) match {
                case 'L' =>
                  if (kdtBRN.left != null && idxRangeLookupInfo.rectSearchRegion.intersects(kdtBRN.left.rectNodeBounds))
                    lstKdtNode += ((kdtBRN.left, !row._2))
                case 'R' =>
                  if (kdtBRN.right != null && idxRangeLookupInfo.rectSearchRegion.intersects(kdtBRN.right.rectNodeBounds))
                    lstKdtNode += ((kdtBRN.right, !row._2))
                case _ =>
                  if (kdtBRN.left != null && idxRangeLookupInfo.rectSearchRegion.intersects(kdtBRN.left.rectNodeBounds))
                    lstKdtNode += ((kdtBRN.left, !row._2))
                  if (kdtBRN.right != null && idxRangeLookupInfo.rectSearchRegion.intersects(kdtBRN.right.rectNodeBounds))
                    lstKdtNode += ((kdtBRN.right, !row._2))
              }

            case kdtLeafNode: KdtLeafNode =>
              if (idxRangeLookupInfo.rectSearchRegion.intersects(kdtLeafNode.rectNodeBounds))
                kdtLeafNode.lstPoints.foreach(updateMatchListAndRegion(_, idxRangeLookupInfo, k))
          }
      )
    }

    process(sPtBestNode, null)

    if (sPtBestNode != kdTree.rootNode) {

      splitX = true
      process(kdTree.rootNode, sPtBestNode)
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

      val sqDistQTPoint = Helper.squaredEuclideanDist(idxRangeLookupInfo.rectSearchRegion.center.x, idxRangeLookupInfo.rectSearchRegion.center.y, point.x, point.y)

      // add point if it's within the search radius
      if (idxRangeLookupInfo.limitNode == null || sqDistQTPoint < idxRangeLookupInfo.sqrDim) {

        idxRangeLookupInfo.sortList.add(sqDistQTPoint, point)

        idxRangeLookupInfo.weight += getNumPoints(point)

        // see if region can shrink if at least the last node can be dropped
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

            idxRangeLookupInfo.rectSearchRegion.halfXY.x = math.sqrt(idxRangeLookupInfo.limitNode.distance) + errorRange
            idxRangeLookupInfo.rectSearchRegion.halfXY.y = idxRangeLookupInfo.rectSearchRegion.halfXY.x

            idxRangeLookupInfo.sqrDim = math.pow(idxRangeLookupInfo.rectSearchRegion.halfXY.x, 2)

            while (elem.next != null && elem.next.distance < idxRangeLookupInfo.sqrDim) {

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