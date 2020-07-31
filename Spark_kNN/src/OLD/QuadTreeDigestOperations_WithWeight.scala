package org.cusp.bdi.sknn.util

import com.insightfullogic.quad_trees.{Box, Point, QuadTreeDigest}
import org.cusp.bdi.util.Helper

import scala.collection.mutable.ListBuffer

object QuadTreeDigestOperations_WithWeight {

  //  private val dimExtend = 3 * math.sqrt(2) // accounts for the further points effected by precision loss when converting to gird

  private val expandBy = 1

  def getNeededSpIdxUId(quadTreeDigest: QuadTreeDigest, searchPointXY: (Long, Long), k: Int /*, gridBoxMaxDim: Double*/): Set[Int] = {

    //        if (searchPointXY._1.toString().startsWith("15221") && searchPointXY._2.toString().startsWith("3205"))
    //            //            //            //            //            //            //        if (searchPointXY._1.toString().startsWith("100648") && searchPointXY._2.toString().startsWith("114152"))
    //            println

    val searchPoint = new Point(searchPointXY._1, searchPointXY._2)

    val sPtBestQTD = getBestQuadrant(quadTreeDigest, searchPoint, k)

    //        val dim = math.ceil(dimExtend + math.max(sPtBestQTD.boundary.halfDimension.x + math.abs(searchPoint.x - sPtBestQTD.boundary.center.x), sPtBestQTD.boundary.halfDimension.y + math.abs(searchPoint.y - sPtBestQTD.boundary.center.y)))
    //        val searchRegion = new Box(searchPoint, new Point(dim, dim))

    //        val searchRegion = new Box(searchPoint, new Point((sPtBestQTD.boundary.halfDimension.x + math.abs(searchPoint.x - sPtBestQTD.boundary.center.x), sPtBestQTD.boundary.halfDimension.y + math.abs(searchPoint.y - sPtBestQTD.boundary.center.y)) /* + gridBoxH */ ))

    //    val dim = math.ceil(/*dimExtend +*/ math.sqrt(getFurthestCorner(searchPoint, sPtBestQTD)._1))

    val dim = (expandBy + math.ceil(math.max(math.abs(searchPoint.x - sPtBestQTD.boundary.left), math.abs(searchPoint.x - sPtBestQTD.boundary.right))),
      expandBy + math.ceil(math.max(math.abs(searchPoint.y - sPtBestQTD.boundary.bottom), math.abs(searchPoint.y - sPtBestQTD.boundary.top))))

    val searchRegion = Box(searchPoint, new Point(dim))

    var sortList = SortedList[Point](Int.MaxValue, true)

    sortList = pointsWithinRegion(sPtBestQTD, quadTreeDigest, searchRegion, k, sortList, null)

    sortList
      .map(_.data.userData.asInstanceOf[(Long, Set[Int])]._2)
      .flatMap(_.seq)
      .toSet
  }

  private def getBestQuadrant(qtDigest: QuadTreeDigest, searchPoint: Point, k: Int) = {

    // find leaf containing point
    var done = false
    var qtd = qtDigest

    def testQuad(qtdChild: QuadTreeDigest) =
      qtdChild != null && qtd.getTotalPointWeight /*qtdChild.getLstPoint.size*/ >= k && qtdChild.boundary.contains(searchPoint.x, searchPoint.y)

    while (!done)
      if (testQuad(qtd.topLeft))
        qtd = qtd.topLeft
      else if (testQuad(qtd.topRight))
        qtd = qtd.topRight
      else if (testQuad(qtd.bottomLeft))
        qtd = qtd.bottomLeft
      else if (testQuad(qtd.bottomRight))
        qtd = qtd.bottomRight
      else
        done = true

    qtd
  }

  private def pointsWithinRegion(qtdStart: QuadTreeDigest, quadTreeDigest: QuadTreeDigest, searchRegion: Box, k: Int, sortList: SortedList[Point], skipQTD: QuadTreeDigest /*, gridBoxMaxDim: Double*/) = {

    var totalWeight = 0L //if (sortList.isEmpty()) 0 else sortList.map(_.data.userData.asInstanceOf[(Long, Set[Int])]._1).sum

    val sortListTemp = sortList
    var prevLastElem: Node[Point] = null //sortList.last()
    var currSqDim = math.pow(searchRegion.halfDimension.x, 2) // if (sortList.isFull) prevLastElem.distance else math.pow(searchRegion.halfDimension.x, 2)
    //    var currMaxDist = math.abs(searchRegion.center.x - searchRegion.left) // + math.abs(searchRegion.center.y - searchRegion.bottom)

    var lstQT = ListBuffer(qtdStart)

    def shrinkSearchRegion() {

      //      if (totalWeight - sortListTemp.last().data.userData.asInstanceOf[(Long, Set[Int])]._1 >= k) {

      if (totalWeight - sortListTemp.last().data.userData.asInstanceOf[(Long, Set[Int])]._1 >= k /* && sortListTemp.last().distance <= currSqDim*/ ) {

        val iter = sortListTemp.iterator()
        var elem = iter.next
        var weightSoFar = elem.data.userData.asInstanceOf[(Long, Set[Int])]._1
        //        var furthestX = elem.data.x
        //        var furthestY = elem.data.y

        var idx = 0
        while (iter.hasNext && weightSoFar < k) {

          idx += 1
          elem = iter.next
          weightSoFar += elem.data.userData.asInstanceOf[(Long, Set[Int])]._1

          //          if (elem.data.x > furthestX)
          //            furthestX = elem.data.x
          //
          //          if (elem.data.y > furthestY)
          //            furthestY = elem.data.y
        }

        if (elem != prevLastElem) {

          prevLastElem = elem

          val newDim = expandBy + math.ceil(math.sqrt(prevLastElem.distance)) /*+ dimExtend*/
          searchRegion.halfDimension.x = newDim
          searchRegion.halfDimension.y = newDim

          currSqDim = math.pow(newDim, 2)

          //          currMaxDist = Helper.manhattanDist(searchRegion.center.x, searchRegion.center.y, furthestX, furthestY)
        }

        // keep points with an extra 1 to account for floor operation. 1 == i unit box
        var continue = true
        if (iter.hasNext)
          do {

            elem = iter.next

            if (elem.distance <= currSqDim)
              idx += 1
            else
              continue = false
          } while (continue && iter.hasNext)

        //          if (elem != prevLastElem) {

        totalWeight = weightSoFar

        sortListTemp.discardAfter(idx + 1)
      }
    }

    def process(startRound: Boolean) {

      lstQT.foreach(qtd => {

        if (startRound || qtd != qtdStart) {

          qtd.getLstPoint
            .foreach(qtPoint => {

              //              if (qtPoint.x.toString().startsWith("16877") && qtPoint.y.toString().startsWith("4163"))
              //                print("")

              if (searchRegion.contains(qtPoint)) {

                val sqDist = Helper.squaredDist(searchRegion.center.x, searchRegion.center.y, qtPoint.x, qtPoint.y)
                // val dist = Helper.manhattanDist(searchRegion.center.x, searchRegion.center.y, qtPoint.x, qtPoint.y)

                if (prevLastElem == null || sqDist <= currSqDim) {

                  sortListTemp.add(sqDist, qtPoint)

                  totalWeight += qtPoint.userData.asInstanceOf[(Long, Set[Int])]._1

                  if (totalWeight > k)
                    shrinkSearchRegion()
                }
              }
            })

          //      else if (qtd.topLeft != null && qtd.topLeft.boundary.contains())
          //        print()
          //      else if (qtd.topRight != null && qtd.topRight.boundary.contains(new Point(6160, 1389)))
          //        print()
          //      else if (qtd.bottomLeft != null && qtd.bottomLeft.boundary.contains(new Point(6160, 1389)))
          //        print()
          //      else if (qtd.bottomRight != null && qtd.bottomRight.boundary.contains(new Point(6160, 1389)))
          //        print()

          if (intersects(qtd.topLeft, searchRegion))
            lstQT += qtd.topLeft
          if (intersects(qtd.topRight, searchRegion))
            lstQT += qtd.topRight
          if (intersects(qtd.bottomLeft, searchRegion))
            lstQT += qtd.bottomLeft
          if (intersects(qtd.bottomRight, searchRegion))
            lstQT += qtd.bottomRight
        }
      })
    }

    process(true)

    if (qtdStart != quadTreeDigest) {

      lstQT = ListBuffer(quadTreeDigest)
      process(false)
    }

    sortListTemp
  }

  private def intersects(qtd: QuadTreeDigest, searchRegion: Box /*, skipQTD: QuadTreeDigest*/) =
    qtd != null /*&& qtd != skipQTD*/ && searchRegion.intersects(qtd.boundary)

  private def getFurthestCorner(searchPoint: Point, sPtBestQTD: QuadTreeDigest) = {

    val qtdLeft = sPtBestQTD.boundary.left
    val qtdRight = sPtBestQTD.boundary.right
    val qtdBottom = sPtBestQTD.boundary.bottom
    val qtdTop = sPtBestQTD.boundary.top

    Array((qtdLeft, qtdBottom), (qtdRight, qtdBottom), (qtdRight, qtdTop), (qtdLeft, qtdTop))
      .map(xy => (Helper.squaredDist(xy._1, xy._2, searchPoint.x, searchPoint.y), xy))
      .maxBy(_._1)
  }
}