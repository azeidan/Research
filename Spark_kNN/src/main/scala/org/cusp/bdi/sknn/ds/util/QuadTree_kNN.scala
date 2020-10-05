//package org.cusp.bdi.sknn.ds.util
//
//import org.cusp.bdi.ds.{DoubleWrapper, QuadTree, SearchRegionInfo, SortedList, SpatialIndex_kNN}
//import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
//import org.cusp.bdi.sknn.GlobalIndexPointData
//import org.cusp.bdi.ds.SpatialIndex_kNN.{testAndAddPoint, updateMatchListAndRegion}
//
//import scala.collection.mutable.ListBuffer
//
//class QuadTree_kNN(_boundary: Rectangle) extends QuadTree(_boundary) with SpatialIndex_kNN {
//
////  def this(mbrMin: (Double, Double), mbrMax: (Double, Double)) {
////
////    this(null)
////
////    val pointHalfXY = new Geom2D(((mbrMax._1 - mbrMin._1) + 1) / 2.0, ((mbrMax._2 - mbrMin._2) + 1) / 2.0)
////
////    this.boundary = Rectangle(new Geom2D(pointHalfXY.x + mbrMin._1, pointHalfXY.y + mbrMin._2), pointHalfXY)
////  }
//
////  def this(mbr: (Double, Double, Double, Double), gridSquareLen: Double) {
////
////    this(null)
////
////    val minX = mbr._1 * gridSquareLen
////    val minY = mbr._2 * gridSquareLen
////    val maxX = mbr._3 * gridSquareLen + gridSquareLen
////    val maxY = mbr._4 * gridSquareLen + gridSquareLen
////
////    val halfWidth = (maxX - minX) / 2
////    val halfHeight = (maxY - minY) / 2
////
////    this.boundary = Rectangle(new Geom2D(halfWidth + minX, halfHeight + minY), new Geom2D(halfWidth, halfHeight))
////  }
//
////  def insert(lstPoints: ListBuffer[Point]): Boolean =
////    insert(lstPoints.iterator)
////
////  override def getTotalPoints: Int = totalPoints
//
////  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point], k: Int) {
////
////    //    if (searchPoint.userData.toString().equalsIgnoreCase("yellow_3_a_772558"))
////    //      println
////
////    var searchRegion: Rectangle = null
////
////    var sPtBestQT: QuadTree = null
////
////    sPtBestQT = getBestQuadrant(searchPoint, k)
////
////    val dim = if (sortSetSqDist.isFull)
////      math.sqrt(sortSetSqDist.last.distance)
////    else
////      math.max(math.max(math.abs(searchPoint.x - sPtBestQT.boundary.left), math.abs(searchPoint.x - sPtBestQT.boundary.right)),
////        math.max(math.abs(searchPoint.y - sPtBestQT.boundary.bottom), math.abs(searchPoint.y - sPtBestQT.boundary.top)))
////
////    searchRegion = Rectangle(searchPoint, new Geom2D(dim, dim))
////
////    pointsWithinRegion(sPtBestQT, searchRegion, sortSetSqDist)
////  }
//
////  private def pointsWithinRegion(sPtBestQT: QuadTree, searchRegion: Rectangle, sortSetSqDist: SortedList[Point]) {
////
////    val prevMaxSqrDist = new DoubleWrapper(if (sortSetSqDist.last == null) -1 else sortSetSqDist.last.distance)
////
////    def process(rootQT: QuadTree, skipQT: QuadTree) {
////
////      val lstQT = ListBuffer(rootQT)
////
////      lstQT.foreach(qTree =>
////        if (qTree != skipQT) {
////
////          qTree.lstPoints.foreach(testAndAddPoint(_, searchRegion, sortSetSqDist, prevMaxSqrDist))
////
////          if (intersects(qTree.topLeft, searchRegion))
////            lstQT += qTree.topLeft
////          if (intersects(qTree.topRight, searchRegion))
////            lstQT += qTree.topRight
////          if (intersects(qTree.bottomLeft, searchRegion))
////            lstQT += qTree.bottomLeft
////          if (intersects(qTree.bottomRight, searchRegion))
////            lstQT += qTree.bottomRight
////        }
////      )
////    }
////
////    if (sPtBestQT != null)
////      process(sPtBestQT, null)
////
////    if (sPtBestQT != this)
////      process(this, sPtBestQT)
////  }
//
////  private def intersects(quadTree: QuadTree, searchRegion: Rectangle) =
////    quadTree != null && searchRegion.intersects(quadTree.boundary)
//
////  override def spatialIdxRangeLookup(searchXY: (Double, Double), k: Int): Set[Int] = {
////
////    //    if (searchPointXY._1.toString().startsWith("26167") && searchPointXY._2.toString().startsWith("4966"))
////    //      println
////
////    val searchPoint = new Geom2D(searchXY._1, searchXY._2)
////
////    val sPtBestQT = getBestQuadrant(searchPoint, k)
////
////    val dim = math.max(math.max(math.abs(searchPoint.x - sPtBestQT.boundary.left), math.abs(searchPoint.x - sPtBestQT.boundary.right)),
////      math.max(math.abs(searchPoint.y - sPtBestQT.boundary.bottom), math.abs(searchPoint.y - sPtBestQT.boundary.top)))
////
////    val searchRegion = Rectangle(searchPoint, new Geom2D(dim, dim))
////
////    spatialIdxRangeLookupHelper(sPtBestQT, searchRegion, k)
////      .map(_.data.userData match {
////        case globalIndexPointData: GlobalIndexPointData => globalIndexPointData.partitionIdx
////      })
////      .toSet
////  }
//
////  private def spatialIdxRangeLookupHelper(sPtBestQT: QuadTree, searchRegion: Rectangle, k: Int) = {
////
////    val sortList = SortedList[Point](Int.MaxValue)
////    val currInfo = new SearchRegionInfo(sortList.head, math.pow(searchRegion.halfXY.x, 2))
////
////    def process(rootQT: QuadTree, skipQT: QuadTree) {
////
////      val lstQT = ListBuffer(rootQT)
////
////      lstQT.foreach(qTree =>
////        if (qTree != skipQT) {
////
////          qTree.lstPoints
////            .foreach(updateMatchListAndRegion(_, searchRegion, sortList, k, currInfo))
////          if (intersects(qTree.topLeft, searchRegion))
////            lstQT += qTree.topLeft
////          if (intersects(qTree.topRight, searchRegion))
////            lstQT += qTree.topRight
////          if (intersects(qTree.bottomLeft, searchRegion))
////            lstQT += qTree.bottomLeft
////          if (intersects(qTree.bottomRight, searchRegion))
////            lstQT += qTree.bottomRight
////        })
////    }
////
////    process(sPtBestQT, null)
////
////    if (sPtBestQT != this)
////      process(this, sPtBestQT)
////
////    sortList
////  }
//
////  private def getBestQuadrant(searchPoint: Geom2D, k: Int): QuadTree = {
////
////    // find leaf containing point
////    var qTree: QuadTree = this
////
////    def testQuad(qtQuad: QuadTree) =
////      qtQuad != null && qtQuad.totalPoints >= k && qtQuad.boundary.contains(searchPoint)
////
////    while (true)
////      if (testQuad(qTree.topLeft))
////        qTree = qTree.topLeft
////      else if (testQuad(qTree.topRight))
////        qTree = qTree.topRight
////      else if (testQuad(qTree.bottomLeft))
////        qTree = qTree.bottomLeft
////      else if (testQuad(qTree.bottomRight))
////        qTree = qTree.bottomRight
////      else
////        return qTree
////
////    null
////  }
//}