package org.cusp.bdi.sknn.util

import scala.collection.mutable.Queue

import org.cusp.bdi.util.Helper

import com.insightfullogic.quad_trees.Box
import com.insightfullogic.quad_trees.Point
import com.insightfullogic.quad_trees.QuadTreeDigest

object QuadTreeDigestOperations {

    private val dimExtend = 3 * math.sqrt(2) // accounts for the further points effected by precision loss when converting to gird

    def getPartitionsInRange(quadTreeDigest: QuadTreeDigest, searchPointXY: (Long, Long), k: Int /*, gridBoxMaxDim: Double*/ ) = {

        //        if (searchPointXY._1.toString().startsWith("15221") && searchPointXY._2.toString().startsWith("3205"))
        //            //            //            //            //            //            //        if (searchPointXY._1.toString().startsWith("100648") && searchPointXY._2.toString().startsWith("114152"))
        //            println

        val searchPoint = new Point(searchPointXY._1, searchPointXY._2)

        val sPtBestQTD = getBestQuadrant(quadTreeDigest, searchPoint, k)

        //        val dim = math.ceil(dimExtend + math.max(sPtBestQTD.boundary.halfDimension.x + math.abs(searchPoint.x - sPtBestQTD.boundary.center.x), sPtBestQTD.boundary.halfDimension.y + math.abs(searchPoint.y - sPtBestQTD.boundary.center.y)))
        //        val searchRegion = new Box(searchPoint, new Point(dim, dim))

        //        val searchRegion = new Box(searchPoint, new Point((sPtBestQTD.boundary.halfDimension.x + math.abs(searchPoint.x - sPtBestQTD.boundary.center.x), sPtBestQTD.boundary.halfDimension.y + math.abs(searchPoint.y - sPtBestQTD.boundary.center.y)) /* + gridBoxH */ ))

        val dim = math.ceil(dimExtend + math.sqrt(getFurthestCorner(searchPoint, sPtBestQTD)._1))

        val searchRegion = new Box(searchPoint, new Point(dim, dim))

        // val sortSetSqDist = SortSetObj(Int.MaxValue)

        var sortSetObj = SortSetObj(Int.MaxValue, true)

        //         sortSetObj = pointsWithinRegion(quadTreeDigest, searchRegion, k, sortSetObj, null, gridBoxW, gridBoxH)

        sortSetObj = pointsWithinRegion(sPtBestQTD, searchRegion, k, sortSetObj, null)

        if (quadTreeDigest != sPtBestQTD)
            sortSetObj = pointsWithinRegion(quadTreeDigest, searchRegion, k, sortSetObj, sPtBestQTD)

        //        println(sortSetObj.size)
        //        val distCap = sortSetSqDist.last.distance + gridBoxSqDiag

        sortSetObj
            .map(_.data match { case pt: Point => pt.userData.asInstanceOf[(Long, Set[Int])]._2 })
            .flatMap(_.seq)
            .toSet
    }

    private def contains(qtd: QuadTreeDigest, searchPoint: Point, k: Int) =
        qtd != null && qtd.getTotalPointWeight > k && qtd.boundary.contains(searchPoint.x, searchPoint.y)

    private def intersects(qtd: QuadTreeDigest, searchRegion: Box, skipQTD: QuadTreeDigest) =
        qtd != null && qtd != skipQTD && searchRegion.intersects(qtd.boundary)

    private def getBestQuadrant(qtDigest: QuadTreeDigest, searchPoint: Point, k: Int) = {

        // find leaf containing point
        var done = false
        var qtd = qtDigest

        while (!done)
            if (contains(qtd.topLeft, searchPoint, k))
                qtd = qtd.topLeft
            else if (contains(qtd.topRight, searchPoint, k))
                qtd = qtd.topRight
            else if (contains(qtd.bottomLeft, searchPoint, k))
                qtd = qtd.bottomLeft
            else if (contains(qtd.bottomRight, searchPoint, k))
                qtd = qtd.bottomRight
            else
                done = true

        qtd
    }

    private def pointsWithinRegion(quadTreeDigest: QuadTreeDigest, searchRegion: Box, k: Int, sortSetObjCurr: SortSetObj, skipQTD: QuadTreeDigest /*, gridBoxMaxDim: Double*/ ) = {

        var totalWeight = if (sortSetObjCurr.isEmpty) 0 else sortSetObjCurr.map(_.data match { case pt: Point => pt.userData.asInstanceOf[(Long, Set[Int])]._1 }).sum

        var sortSetObj = sortSetObjCurr
        var prevLastElem = if (sortSetObjCurr.isEmpty) null else sortSetObjCurr.last
        var currSqDim = if (sortSetObjCurr.isEmpty) 0 else prevLastElem.distance + dimExtend

        def shrinkSearchRegion() {

            if (totalWeight - (sortSetObj.last.data match { case pt: Point => pt.userData.asInstanceOf[(Long, Set[Int])]._1 }) >= k) {

                val iter = sortSetObj.iterator
                var elem = iter.next
                var weightSoFar = elem.data match { case pt: Point => pt.userData.asInstanceOf[(Long, Set[Int])]._1 }

                var idx = 0
                while (iter.hasNext && (weightSoFar < k || elem.distance == 0)) {

                    idx += 1
                    elem = iter.next
                    weightSoFar += (elem.data match { case pt: Point => pt.userData.asInstanceOf[(Long, Set[Int])]._1 })
                }

                if (elem != prevLastElem) {

                    prevLastElem = elem

                    val newDim = math.sqrt(elem.distance) + dimExtend
                    searchRegion.halfDimension.x = newDim
                    searchRegion.halfDimension.y = newDim

                    currSqDim = math.pow(newDim, 2)

                    if (idx < sortSetObj.size - 1 && sortSetObj.last.distance > currSqDim) {

                        var done = false
                        idx += 1

                        do {

                            elem = iter.next

                            if (elem.distance <= currSqDim) {

                                weightSoFar += (elem.data match { case pt: Point => pt.userData.asInstanceOf[(Long, Set[Int])]._1 })
                                idx += 1
                            }
                            else
                                done = true
                        } while (!done && idx < sortSetObj.size)

                        if (idx < sortSetObj.size) {

                            totalWeight = weightSoFar

                            sortSetObj.discardAfter(idx)
                        }
                    }
                }
            }
        }

        val queueQT = Queue(quadTreeDigest)

        while (!queueQT.isEmpty) {

            val qtd = queueQT.dequeue()

            //            if (qtd.boundary.center.x.toString().startsWith("15206") && qtd.boundary.center.y.toString().startsWith("3362"))
            //                println

            qtd.getLstPoint
                .foreach(qtPoint => {

                    //                    if (qtPoint.x.toString().startsWith("15218") && qtPoint.y.toString().startsWith("3204"))
                    //                        println

                    if (searchRegion.contains(qtPoint)) {

                        val sqDist = Helper.squaredDist(searchRegion.center.x, searchRegion.center.y, qtPoint.x, qtPoint.y)

                        //                        val mDist = math.abs(searchRegion.center.x - qtPoint.x) + math.abs(searchRegion.center.y - qtPoint.y)

                        if (prevLastElem == null || sqDist <= currSqDim) {

                            sortSetObj.add(sqDist, qtPoint) // PointWithUniquId(qtd.getSetPart, qtPoint))

                            totalWeight += qtPoint.userData.asInstanceOf[(Long, Set[Int])]._1

                            if (totalWeight > k)
                                shrinkSearchRegion()
                        }
                    }
                })

            if (intersects(qtd.topLeft, searchRegion, skipQTD))
                queueQT += qtd.topLeft
            if (intersects(qtd.topRight, searchRegion, skipQTD))
                queueQT += qtd.topRight
            if (intersects(qtd.bottomLeft, searchRegion, skipQTD))
                queueQT += qtd.bottomLeft
            if (intersects(qtd.bottomRight, searchRegion, skipQTD))
                queueQT += qtd.bottomRight
        }

        sortSetObj
    }

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