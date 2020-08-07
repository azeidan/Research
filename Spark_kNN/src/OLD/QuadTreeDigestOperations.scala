//package org.cusp.bdi.sknn.util
//
//import com.insightfullogic.quad_trees.{Box, Point, QuadTree}
//import org.cusp.bdi.util.Helper
//
//import scala.collection.mutable.ListBuffer
//
//object QuadTreeDigestOperations {
//
//  //  private val dimExtend = 3 * math.sqrt(2) // accounts for the further points effected by precision loss when converting to gird
//
//  private val expandBy = math.sqrt(2)
//
//  def spatialIdxRangeLookup(quadree: QuadTree, searchPointXY: (Long, Long), k: Int ): Set[Int] = {
//
//    //        if (searchPointXY._1.toString().startsWith("15221") && searchPointXY._2.toString().startsWith("3205"))
//    //            //            //            //            //            //            //        if (searchPointXY._1.toString().startsWith("100648") && searchPointXY._2.toString().startsWith("114152"))
//    //            println
//
//    val searchPoint = new Point(searchPointXY._1, searchPointXY._2)
//
//    val sPtBestQTD = QuadTreeOperations.getBestQuadrant(quadree, searchPoint, k)
//
//    //        val dim = math.ceil(dimExtend + math.max(sPtBestQTD.boundary.halfDimension.x + math.abs(searchPoint.x - sPtBestQTD.boundary.center.x), sPtBestQTD.boundary.halfDimension.y + math.abs(searchPoint.y - sPtBestQTD.boundary.center.y)))
//    //        val searchRegion = new Box(searchPoint, new Point(dim, dim))
//
//    //        val searchRegion = new Box(searchPoint, new Point((sPtBestQTD.boundary.halfDimension.x + math.abs(searchPoint.x - sPtBestQTD.boundary.center.x), sPtBestQTD.boundary.halfDimension.y + math.abs(searchPoint.y - sPtBestQTD.boundary.center.y)) /* + gridBoxH */ ))
//
//    //    val dim = math.ceil(/*dimExtend +*/ math.sqrt(getFurthestCorner(searchPoint, sPtBestQTD)._1))
//
//    val dim = (expandBy + (math.max(math.abs(searchPoint.x - sPtBestQTD.boundary.left), math.abs(searchPoint.x - sPtBestQTD.boundary.right))),
//      expandBy + (math.max(math.abs(searchPoint.y - sPtBestQTD.boundary.bottom), math.abs(searchPoint.y - sPtBestQTD.boundary.top))))
//
//    val searchRegion = Box(searchPoint, new Point(dim))
//
//    val sortList = pointsWithinRegion(sPtBestQTD, quadree, searchRegion, k, null)
//
//    sortList
//      .map(_.data.userData match { case s: Set[Int] => s })
//      .flatMap(_.seq)
//      .toSet
//  }
//
//  //  private def getBestQuadrant(quadTree: QuadTree, searchPoint: Point, k: Int) = {
//  //
//  //    // find leaf containing point
//  //    var done = false
//  //    var qTree = quadTree
//  //
//  //    def testQuad(qtQuad: QuadTree) =
//  //      qtQuad != null && qtQuad.getTotalPoints >= k && qtQuad.boundary.contains(searchPoint.x, searchPoint.y)
//  //
//  //    while (!done)
//  //      if (testQuad(qTree.topLeft))
//  //        qTree = qTree.topLeft
//  //      else if (testQuad(qTree.topRight))
//  //        qTree = qTree.topRight
//  //      else if (testQuad(qTree.bottomLeft))
//  //        qTree = qTree.bottomLeft
//  //      else if (testQuad(qTree.bottomRight))
//  //        qTree = qTree.bottomRight
//  //      else
//  //        done = true
//  //
//  //    qTree
//  //  }
//
//  private def pointsWithinRegion(quadTreeStart: QuadTree, quadTree: QuadTree, searchRegion: Box, k: Int, skipQTD: QuadTree) = {
//
//    var totalCount = 0 //if (sortList.isEmpty()) 0 else sortList.map(_.data.userData.asInstanceOf[(Long, Set[Int])]._1).sum
//
//    val sortList = SortedList[Point](Int.MaxValue, true)
//    var prevLastElem: Node[Point] = null
//    var currSqDim = math.pow(searchRegion.halfDimension.x, 2)
//
//    var lstQT = ListBuffer(quadTreeStart)
//
//    def shrinkSearchRegion() {
//
//      //      if (totalCount - sortList.last().data.userData.asInstanceOf[(Long, Set[Int])]._1 >= k) {
//
//      //      if (totalCount - 1 >= k /* && sortList.last().distance <= currSqDim*/ ) {
//
//      val iter = sortList.iterator()
//      var elem = iter.next
//
//      var idx = 1
//      while (idx < k) {
//
//        idx += 1
//        elem = iter.next
//      }
//
//      if (elem != prevLastElem) {
//
//        prevLastElem = elem
//
//        val newDim = expandBy + math.sqrt(prevLastElem.distance) /*+ dimExtend*/
//        searchRegion.halfDimension.x = newDim
//        searchRegion.halfDimension.y = newDim
//
//        currSqDim = math.pow(newDim, 2)
//      }
//
//      // keep points within new dimension
//      var continue = true
//      if (iter.hasNext)
//        do {
//
//          elem = iter.next
//
//          if (elem.distance <= currSqDim)
//            idx += 1
//          else
//            continue = false
//        } while (continue && iter.hasNext)
//
//      totalCount = idx
//
//      sortList.discardAfter(idx + 1)
//    }
//
//    def process(startRound: Boolean) {
//
//      lstQT.foreach(qTree => {
//
//        if (startRound || qTree != quadTreeStart) {
//
//          qTree.getLstPoint
//            .foreach(qtPoint => {
//
//              //              if (qtPoint.x.toString().startsWith("16877") && qtPoint.y.toString().startsWith("4163"))
//              //                print("")
//
//              if (searchRegion.contains(qtPoint)) {
//
//                val sqDist = Helper.squaredDist(searchRegion.center.x, searchRegion.center.y, qtPoint.x, qtPoint.y)
//                // val dist = Helper.manhattanDist(searchRegion.center.x, searchRegion.center.y, qtPoint.x, qtPoint.y)
//
//                if (prevLastElem == null || sqDist <= currSqDim) {
//
//                  sortList.add(sqDist, qtPoint)
//
//                  totalCount += 1
//
//                  if (totalCount > k)
//                    shrinkSearchRegion()
//                }
//              }
//            })
//
//          //      else if (qtd.topLeft != null && qtd.topLeft.boundary.contains())
//          //        print()
//          //      else if (qtd.topRight != null && qtd.topRight.boundary.contains(new Point(6160, 1389)))
//          //        print()
//          //      else if (qtd.bottomLeft != null && qtd.bottomLeft.boundary.contains(new Point(6160, 1389)))
//          //        print()
//          //      else if (qtd.bottomRight != null && qtd.bottomRight.boundary.contains(new Point(6160, 1389)))
//          //        print()
//
//          if (intersects(qTree.topLeft, searchRegion))
//            lstQT += qTree.topLeft
//          if (intersects(qTree.topRight, searchRegion))
//            lstQT += qTree.topRight
//          if (intersects(qTree.bottomLeft, searchRegion))
//            lstQT += qTree.bottomLeft
//          if (intersects(qTree.bottomRight, searchRegion))
//            lstQT += qTree.bottomRight
//        }
//      })
//    }
//
//    process(true)
//
//    if (quadTreeStart != quadTree) {
//
//      lstQT = ListBuffer(quadTree)
//      process(false)
//    }
//
//    sortList
//  }
//
////  private def getFurthestCorner(searchPoint: Point, sPtBestQTD: QuadTree) = {
////
////    val qtdLeft = sPtBestQTD.boundary.left
////    val qtdRight = sPtBestQTD.boundary.right
////    val qtdBottom = sPtBestQTD.boundary.bottom
////    val qtdTop = sPtBestQTD.boundary.top
////
////    Array((qtdLeft, qtdBottom), (qtdRight, qtdBottom), (qtdRight, qtdTop), (qtdLeft, qtdTop))
////      .map(xy => (Helper.squaredDist(xy._1, xy._2, searchPoint.x, searchPoint.y), xy))
////      .maxBy(_._1)
////  }
//}