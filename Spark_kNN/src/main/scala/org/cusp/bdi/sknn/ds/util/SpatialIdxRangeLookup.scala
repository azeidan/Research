package org.cusp.bdi.sknn.ds.util

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

  case class SearchRegionInfo(rectSearchRegion: Rectangle, errorRange: Float) {

    val sortList: SortedList[Point] = new SortedList[Point]()
    var limitNode: Node[Point] = _
    var sqDim: Double = math.pow(rectSearchRegion.halfXY.x, 2)
    var weight: Long = 0L
  }

  val errorRange: Float = math.sqrt(8).toFloat

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

  def lookup(quadTree: QuadTree, searchXY: (Double, Double), k: Int): SortedList[Point] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    val searchPoint = new Geom2D(searchXY._1, searchXY._2)

    val sPtBestQT = quadTree.getBestQuadrant(searchPoint, k)

    val dim = math.max(math.max(math.abs(searchPoint.x - sPtBestQT.boundary.left), math.abs(searchPoint.x - sPtBestQT.boundary.right)),
      math.max(math.abs(searchPoint.y - sPtBestQT.boundary.bottom), math.abs(searchPoint.y - sPtBestQT.boundary.top)))

    val rectSearchRegion = Rectangle(searchPoint, new Geom2D(dim, dim))

    val searchRegionInfo = SearchRegionInfo(rectSearchRegion, errorRange)

    lookupHelper(sPtBestQT, quadTree, searchRegionInfo, k)

    searchRegionInfo.sortList
  }

  private def lookupHelper(sPtBestQT: QuadTree, quadTree: QuadTree, searchRegionInfo: SearchRegionInfo, k: Int) {

    def process(rootQT: QuadTree, skipQT: QuadTree) {

      val lstQT = ListBuffer(rootQT)

      lstQT.foreach(qt =>
        if (qt != skipQT) {

          qt.lstPoints
            .foreach(updateMatchListAndRegion(_, searchRegionInfo, k))
          if (QuadTree.intersects(qt.topLeft, searchRegionInfo.rectSearchRegion))
            lstQT += qt.topLeft
          if (QuadTree.intersects(qt.topRight, searchRegionInfo.rectSearchRegion))
            lstQT += qt.topRight
          if (QuadTree.intersects(qt.bottomLeft, searchRegionInfo.rectSearchRegion))
            lstQT += qt.bottomLeft
          if (QuadTree.intersects(qt.bottomRight, searchRegionInfo.rectSearchRegion))
            lstQT += qt.bottomRight
        })
    }

    process(sPtBestQT, null)

    if (sPtBestQT != quadTree)
      process(quadTree, sPtBestQT)
  }

  def lookup(kdTree: KdTree, searchXY: (Double, Double), k: Int): SortedList[Point] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    val searchPoint = new Geom2D(searchXY._1, searchXY._2)

    val sPtBestNode = kdTree.getBestNode(searchPoint, k)

    val dim = math.max(math.max(math.abs(searchPoint.x - sPtBestNode.rectMBR.left), math.abs(searchPoint.x - sPtBestNode.rectMBR.right)),
      math.max(math.abs(searchPoint.y - sPtBestNode.rectMBR.bottom), math.abs(searchPoint.y - sPtBestNode.rectMBR.top)))

    val rectSearchRegion = Rectangle(searchPoint, new Geom2D(dim, dim))

    val searchRegionInfo = SearchRegionInfo(rectSearchRegion, errorRange)

    lookupHelper(sPtBestNode, kdTree, searchRegionInfo, k)

    searchRegionInfo.sortList
  }

  private def lookupHelper(sPtBestNode: KdtNode, kdTree: KdTree, searchRegionInfo: SearchRegionInfo, k: Int): Unit = {

    def process(branchRootNode: KdtNode, skipBranchRootNode: KdtNode) {

      val stackNode = mutable.Stack(branchRootNode)

      while (stackNode.nonEmpty) {

        val node = stackNode.pop

        if (node != skipBranchRootNode && searchRegionInfo.rectSearchRegion.intersects(node.rectMBR)) {

          node.arrPoints.foreach(updateMatchListAndRegion(_, searchRegionInfo, k))

          node match {
            case brn: KdtBranchRootNode =>

              if (brn.left != null && brn.left.rectMBR.intersects(searchRegionInfo.rectSearchRegion))
                stackNode.push(brn.left)

              if (brn.right != null && brn.right.rectMBR.intersects(searchRegionInfo.rectSearchRegion))
                stackNode.push(brn.right)

            case _ =>
          }
        }
      }
    }

    process(sPtBestNode, null)

    if (sPtBestNode != kdTree.root)
      process(kdTree.root, sPtBestNode)
  }

  private def updateMatchListAndRegion(point: Point, searchRegionInfo: SearchRegionInfo, k: Int): Unit = {

    def getNumPoints(point: Point): Long = point.userData match {
      case pointData: PointData => pointData.numPoints
      case _ => throw new ClassCastException("Point.userdata must extend " + classOf[PointData].getName)
    }

    if (searchRegionInfo.rectSearchRegion.contains(point)) {

      //              if (qtPoint.x.toString().startsWith("26157") && qtPoint.y.toString().startsWith("4965"))
      //                print("")

      val sqDistQTPoint = Helper.squaredDist(searchRegionInfo.rectSearchRegion.center.x, searchRegionInfo.rectSearchRegion.center.y, point.x, point.y)

      // add point if it's within the search radius
      if (searchRegionInfo.limitNode == null || sqDistQTPoint < searchRegionInfo.sqDim) {

        searchRegionInfo.sortList.add(sqDistQTPoint, point)

        searchRegionInfo.weight += getNumPoints(point)

        // see if region can shrink if at least the last node can be dropped
        if ((searchRegionInfo.limitNode == null || searchRegionInfo.sortList.last.data != point) && (searchRegionInfo.weight - getNumPoints(searchRegionInfo.sortList.last.data)) >= k) {

          var elem = searchRegionInfo.sortList.head
          var newWeight = getNumPoints(elem.data)

          while (newWeight < k) {

            elem = elem.next
            newWeight += getNumPoints(elem.data)
          }

          if (searchRegionInfo.limitNode != elem) {

            searchRegionInfo.limitNode = elem

            searchRegionInfo.rectSearchRegion.halfXY.x = math.sqrt(searchRegionInfo.limitNode.distance) + searchRegionInfo.errorRange
            searchRegionInfo.rectSearchRegion.halfXY.y = searchRegionInfo.rectSearchRegion.halfXY.x

            searchRegionInfo.sqDim = math.pow(searchRegionInfo.rectSearchRegion.halfXY.x, 2)

            while (elem.next != null && elem.next.distance < searchRegionInfo.sqDim) {

              elem = elem.next
              newWeight += getNumPoints(elem.data)
            }

            searchRegionInfo.sortList.stopAt(elem)
            searchRegionInfo.weight = newWeight
          }
        }
      }
    }
  }
}
