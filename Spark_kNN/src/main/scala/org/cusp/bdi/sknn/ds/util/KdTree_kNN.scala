package org.cusp.bdi.sknn.ds.util

import org.cusp.bdi.ds.kt.{KdTree, KdtBranchRootNode, KdtNode}
import org.cusp.bdi.ds.{Geom2D, Point, Rectangle}
import org.cusp.bdi.sknn.GlobalIndexPointData
import org.cusp.bdi.sknn.ds.util.SpatialIndex_kNN.{testAndAddPoint, updateMatchListAndRegion}
import org.cusp.bdi.util.SortedList

import scala.collection.mutable

class KdTree_kNN() extends KdTree with SpatialIndex_kNN {

  override def insert(iterPoints: Iterator[Point]): Boolean =
    super.insert(iterPoints.toArray)

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

        if (node != skipBranchRootNode && searchRegion.intersects(node.rectMBR))
          node match {
            case brn: KdtBranchRootNode =>

              brn.arrSplitPoints.foreach(testAndAddPoint(_, searchRegion, sortSetSqDist, prevMaxSqrDist))

              if (brn.left.rectMBR.intersects(searchRegion))
                stackNode.push(brn.left)

              if (brn.right.rectMBR.intersects(searchRegion))
                stackNode.push(brn.right)

            case node: KdtNode =>
              node.arrPoints.foreach(testAndAddPoint(_, searchRegion, sortSetSqDist, prevMaxSqrDist))
          }
      }
    }

    if (sPtBestNode != null)
      process(sPtBestNode, null)

    if (sPtBestNode != this.root)
      process(this.root, sPtBestNode)
  }

  override def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int): Set[Int] = {

    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
    //      println

    val searchPoint = new Geom2D(searchXY._1, searchXY._2)

    val sPtBestNode = getBestNode(searchPoint, k)

    val dim = math.max(math.max(math.abs(searchPoint.x - sPtBestNode.rectMBR.left), math.abs(searchPoint.x - sPtBestNode.rectMBR.right)),
      math.max(math.abs(searchPoint.y - sPtBestNode.rectMBR.bottom), math.abs(searchPoint.y - sPtBestNode.rectMBR.top)))

    val searchRegion = Rectangle(searchPoint, new Geom2D(dim, dim))

    spatialIdxRangeLookupHelper(sPtBestNode, searchRegion, k)
      .map(_.data.userData match {
        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
      })
      .toSet
  }

  //  private def intersects(kdtNode: KdtNode, searchRegion: Rectangle) =
  //    kdtNode != null && searchRegion.intersects(kdtNode.rectMBR)

  private def spatialIdxRangeLookupHelper(sPtBestNode: KdtNode, searchRegion: Rectangle, k: Int) = {

    val sortList = SortedList[Point](Int.MaxValue)
    val searchRegionInfo = new SearchRegionInfo(sortList.head, math.pow(searchRegion.halfXY.x, 2))

    def process(branchRootNode: KdtNode, skipBranchRootNode: KdtNode) {

      val stackNode = mutable.Stack(branchRootNode)

      while (stackNode.nonEmpty) {

        val node = stackNode.pop

        if (node != skipBranchRootNode && searchRegion.intersects(node.rectMBR))
          node match {
            case brn: KdtBranchRootNode =>

              brn.arrSplitPoints.foreach(updateMatchListAndRegion(_, searchRegion, sortList, k, searchRegionInfo))

              if (brn.left.rectMBR.intersects(searchRegion))
                stackNode.push(brn.left)

              if (brn.right.rectMBR.intersects(searchRegion))
                stackNode.push(brn.right)

            case node: KdtNode =>
              node.arrPoints.foreach(updateMatchListAndRegion(_, searchRegion, sortList, k, searchRegionInfo))
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