package com.insightfullogic.quad_trees

import scala.collection.Iterable
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.MutableList

import QuadTree.capacity

object QuadTree {

    val capacity = 4

    //    def main(args: Array[String]): Unit = {
    //
    //        val qt = QuadTreeSummary(Box(Point(20, 20), Point(20, 20)))
    //
    //        qt.insert(Point(1, 1))
    //        qt.insert(Point(2, 2))
    //        qt.insert(Point(3, 3))
    //        qt.insert(Point(4, 4))
    //        qt.insert(Point(6, 6))
    //
    //        println(qt.getTotalPointsCount())
    //    }
}

case class QuadTreeAddendum() {

    val points = MutableList[Point]()
    var marker = false

    def pointsMBR() = {
        val (minX, minY, maxX, maxY) = points.map(point => (point.x, point.y, point.x, point.y))
            .fold((Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))((x, y) =>
                (math.min(x._1, y._1), math.min(x._2, y._2), math.max(x._3, y._3), math.max(x._4, y._4)))

        val halfDimension = new Point((maxX - minX) / 2, (maxY - minY) / 2)

        Box(new Point(minX + halfDimension.x, minY + halfDimension.y), halfDimension)
    }
}

case class QuadTree(boundary: Box) extends Serializable {

    var assignedPart: Int = -1
    var totalPoints = 0L
    var parent: QuadTree = null
    var topLeft: QuadTree = null
    var topRight: QuadTree = null
    var bottomLeft: QuadTree = null
    var bottomRight: QuadTree = null
    var pointsMBR: Box = null
    var qTreeAddendum = QuadTreeAddendum()
    var arrSegments: Array[(Long, Box)] = null

    private var innerBoundary: Box = null

    def getInnerBoundary = {

        if (innerBoundary == null) {

            var qTree = this
            val lstQT = ListBuffer(qTree)

            var idx = 0
            var minX = Double.MaxValue
            var minY = Double.MaxValue
            var maxX = Double.MinValue
            var maxY = Double.MinValue

            while (idx < lstQT.size) {

                val qt = lstQT(idx)

                val (pointMinX, pointMinY, pointMaxX, pointMaxY) = (qt.pointsMBR.center.x - qt.pointsMBR.halfDimension.x,
                    qt.pointsMBR.center.y - qt.pointsMBR.halfDimension.y,
                    qt.pointsMBR.center.x + qt.pointsMBR.halfDimension.x,
                    qt.pointsMBR.center.y + qt.pointsMBR.halfDimension.y)

                if (pointMinX < minX) minX = pointMinX
                if (pointMinY < minY) minY = pointMinY
                if (pointMaxX > maxX) maxX = pointMaxX
                if (pointMaxY > maxY) maxY = pointMaxY

                if (qt.topLeft != null) lstQT.append(qt.topLeft)
                if (qt.topRight != null) lstQT.append(qt.topRight)
                if (qt.bottomLeft != null) lstQT.append(qt.bottomLeft)
                if (qt.bottomRight != null) lstQT.append(qt.bottomRight)

                idx += 1
            }

            val widthHalf = ((maxX - minX) / 2) + 0.1
            val heightHalf = ((maxY - minY) / 2) + 0.1
            innerBoundary = Box(new Point(widthHalf + minX, heightHalf + minY), new Point(widthHalf, heightHalf))
        }

        innerBoundary
    }

    //    def totalPoints = qTreeAddendum.totalPoints
    def points = qTreeAddendum.points

    def this(boundary: Box, parent: QuadTree) = {

        this(boundary)

        this.parent = parent
    }

    //    def setUniqueId(uniqueID: String) {
    //        qTreeAddendum.uniqueID = uniqueID
    //    }

    //    def printAll() {
    //
    //        val lst = ListBuffer(this)
    //
    //        lstQT.foreach(qTree => {
    //
    //        })
    //
    //        var idx = 0
    //        while (idx < lst.size) {
    //
    //            //            lst(idx).points.foreach(x =>
    //            //                println(">>%s\t%s".format(x, if (withUserData) x.getUserData.toString() else "")))
    //
    //            if (lst(idx).topLeft != null) lst.append(lst(idx).topLeft)
    //            if (lst(idx).topRight != null) lst.append(lst(idx).topRight)
    //            if (lst(idx).bottomLeft != null) lst.append(lst(idx).bottomLeft)
    //            if (lst(idx).bottomRight != null) lst.append(lst(idx).bottomRight)
    //
    //            idx += 1
    //        }
    //    }

    def printAllBoundaries() {
        printAllBoundaries(this)
    }

    def printAllBoundaries(qTree: QuadTree) {

        if (qTree != null) {
            println(qTree.toString)

            if (!qTree.isLeaf) {

                printAllBoundaries(qTree.topLeft)
                printAllBoundaries(qTree.topRight)
                printAllBoundaries(qTree.bottomLeft)
                printAllBoundaries(qTree.bottomRight)
            }
        }
    }

    //    def insert(lst: Iterable[(Double, Double)]): Unit =
    //        lst.foreach(xy => if (!insert(new Point(xy._1, xy._2)))
    //            throw new Exception("Insert failed " + xy))

    def insert(iterPoints: Iterable[Point]): Unit =
        iterPoints.foreach(point => if (!insert(point))
            throw new Exception("Insert failed " + point))

    def insert(point: Point): Boolean = {

        var qTree = this

        // for the Double precision problem
        lazy val pointLeft = math.ceil(point.x)
        lazy val pointRight = math.floor(point.x)
        lazy val pointBottom = math.ceil(point.y)
        lazy val pointTop = math.floor(point.y)

        while (!(pointLeft < qTree.boundary.left || pointRight > qTree.boundary.right ||
            pointBottom < qTree.boundary.bottom || pointTop > qTree.boundary.top)) {

            qTree.totalPoints += 1

            if (qTree.points.size < capacity) {

                qTree.points += point
                return true
            }
            else {

                // switch to proper quadrant?

                qTree = if (pointRight <= qTree.boundary.center.x)
                    if (pointBottom >= qTree.boundary.center.y) {
                        if (qTree.topLeft == null) qTree.topLeft = new QuadTree(qTree.boundary.topLeftQuadrant(), qTree)
                        qTree.topLeft
                    }
                    else {
                        if (qTree.bottomLeft == null) qTree.bottomLeft = new QuadTree(qTree.boundary.bottomLeftQuadrant(), qTree)
                        qTree.bottomLeft
                    }
                else if (pointBottom >= qTree.boundary.center.y) {
                    if (qTree.topRight == null) qTree.topRight = new QuadTree(qTree.boundary.topRightQuadrant(), qTree)
                    qTree.topRight
                }
                else {
                    if (qTree.bottomRight == null) qTree.bottomRight = new QuadTree(qTree.boundary.bottomRightQuadrant(), qTree)
                    qTree.bottomRight
                }
            }
        }

        false
    }

    private def markAll(qTree: QuadTree) {

        val lstQT = ListBuffer(qTree)

        lstQT.foreach(qt => {

            qt.qTreeAddendum.marker = true

            if (qt.topLeft != null && !qt.topLeft.qTreeAddendum.marker) lstQT.append(qt.topLeft)
            if (qt.topRight != null && !qt.topRight.qTreeAddendum.marker) lstQT.append(qt.topRight)
            if (qt.bottomLeft != null && !qt.bottomLeft.qTreeAddendum.marker) lstQT.append(qt.bottomLeft)
            if (qt.bottomRight != null && !qt.bottomRight.qTreeAddendum.marker) lstQT.append(qt.bottomRight)
        })
    }

    private def classifyQuads(qTree: QuadTree) = {

        var lstMarkedQuads = ListBuffer[QuadTree]()
        var lstUnMarkedQuads = ListBuffer[QuadTree]()

        if (qTree.topLeft != null)
            if (qTree.topLeft.qTreeAddendum.marker)
                lstMarkedQuads += qTree.topLeft
            else
                lstUnMarkedQuads += qTree.topLeft

        if (qTree.topRight != null)
            if (qTree.topRight.qTreeAddendum.marker)
                lstMarkedQuads += qTree.topRight
            else
                lstUnMarkedQuads += qTree.topRight

        if (qTree.bottomLeft != null)
            if (qTree.bottomLeft.qTreeAddendum.marker)
                lstMarkedQuads += qTree.bottomLeft
            else
                lstUnMarkedQuads += qTree.bottomLeft

        if (qTree.bottomRight != null)
            if (qTree.bottomRight.qTreeAddendum.marker)
                lstMarkedQuads += qTree.bottomRight
            else
                lstUnMarkedQuads += qTree.bottomRight

        (lstMarkedQuads, lstUnMarkedQuads)
    }

    def computeSubRegions(maxPointCount: Long) {

        // go to leaf with fewer than maxPointCount
        var lstSegments = ListBuffer[(Long, Box)]()
        //        var qTree = this // queryPoint(boundary.center)
        var added = 0L

        //        val lstQT = ListBuffer(this)
        //
        //        lstQT.foreach(qt => {
        //
        //            if (!qt.qTreeAddendum.marker) {
        //
        //                val lstMarkedQuads = getQuadrants(qt).filter(_.qTreeAddendum.marker)
        //
        //                val processedPointCount = if (lstMarkedQuads.size == 0) 0 else lstMarkedQuads.map(_.totalPoints).sum
        //
        //                if (qt.totalPoints - processedPointCount <= maxPointCount) {
        //
        //                    var qTree = if (qt.parent == null) qt else qt.parent
        //
        //                    val lst2 = getQuadrants(qTree)
        //                        .filter(_ != null)
        //                        .filter(_.qTreeAddendum.marker)
        //                    val pCount = if (lst2.size == 0) 0 else lst2.map(_.totalPoints).sum
        //
        //                    lstSegments.append((qTree.totalPoints - pCount, qTree.boundary))
        //                    markAll(qTree)
        //                }
        //                else {
        //
        //                    if (qt.topLeft != null && !qt.topLeft.qTreeAddendum.marker) lstQT.append(qt.topLeft)
        //                    if (qt.topRight != null && !qt.topRight.qTreeAddendum.marker) lstQT.append(qt.topRight)
        //                    if (qt.bottomLeft != null && !qt.bottomLeft.qTreeAddendum.marker) lstQT.append(qt.bottomLeft)
        //                    if (qt.bottomRight != null && !qt.bottomRight.qTreeAddendum.marker) lstQT.append(qt.bottomRight)
        //                }
        //            }
        //        })

        var qTree = this
        while (qTree != null) {

            var (lstMarkedQuads, lstUnMarkedQuads) = classifyQuads(qTree)
            var processedPointCount = if (lstMarkedQuads.size == 0) 0 else lstMarkedQuads.map(_.totalPoints).sum

            var bestQuad = (qTree.totalPoints - processedPointCount, qTree, lstUnMarkedQuads)

            var done = false

            while (!done) {
                //                if (bestQuad._2.totalPoints == 2410)
                //                    println
                done = true

                if (bestQuad._3.size > 0) {

                    val bestQuad2 = bestQuad._3.map(umq => {

                        val (lstMarkedQuads2, lstUnMarkedQuads2) = classifyQuads(umq)

                        var count = Long.MinValue

                        if (lstUnMarkedQuads2.size != 0) {

                            val processedPointCount2 = if (lstMarkedQuads2.size == 0) 0 else lstMarkedQuads2.map(_.totalPoints).sum

                            count = umq.totalPoints - processedPointCount2
                        }

                        (count, umq, lstUnMarkedQuads2)
                    })
                        .maxBy(_._1)

                    //                    val (lstMarkedQuads2, lstUnMarkedQuads2) = classifyQuads(bestQuad2)
                    //                    val processedPointCount2 = if (lstMarkedQuads2.size == 0) 0 else lstMarkedQuads2.map(_.totalPoints).sum

                    if (bestQuad2._1 > maxPointCount) {

                        bestQuad = bestQuad2

                        done = false
                    }
                }

                println(bestQuad)
            }
            //            }

            //            qTree = qTree.parent
            //            lsts = getQuadrants(qTree)
            //            lstMarkedQuads = lsts._1
            //            lstUnMarkedQuads = lsts._2
            println(bestQuad)

            //            if (lstUnMarkedQuads.size > 0 && lstUnMarkedQuads.map(_.totalPoints).max <= maxPointCount) {
            //            processedPointCount = if (lstMarkedQuads.size == 0) 0 else lstMarkedQuads.map(_.totalPoints).sum

            lstSegments.append((bestQuad._1, bestQuad._2.boundary))
            markAll(bestQuad._2)

            if (bestQuad._2.parent == null)
                qTree = null
            //            }

            //            requiredCount += qTree.totalPoints
        }

        arrSegments = lstSegments.toArray
    }

    /**
      * Removes the addendum data object from every quadrant and converts
      * each quadrants points to their MBR.
      * The process is irreversible
      */
    def summarize() = {

        val lstQT = ListBuffer(this)

        lstQT.foreach(qTree => {

            qTree.pointsMBR = qTree.qTreeAddendum.pointsMBR()

            qTree.qTreeAddendum = null

            if (qTree.topLeft != null) lstQT.append(qTree.topLeft)
            if (qTree.topRight != null) lstQT.append(qTree.topRight)
            if (qTree.bottomLeft != null) lstQT.append(qTree.bottomLeft)
            if (qTree.bottomRight != null) lstQT.append(qTree.bottomRight)
        })

        //        do {
        //
        //            qTree = lstQT(idx)
        //
        //            qTree.pointsMBR = qTree.qTreeAddendum.getPointsMBR()
        //
        //            qTree.qTreeAddendum = null
        //
        //            if (qTree.topLeft != null) lstQT.append(qTree.topLeft)
        //            if (qTree.topRight != null) lstQT.append(qTree.topRight)
        //            if (qTree.bottomLeft != null) lstQT.append(qTree.bottomLeft)
        //            if (qTree.bottomRight != null) lstQT.append(qTree.bottomRight)
        //
        //            idx += 1
        //        } while (idx < lstQT.size)
    }

    def getAllPoints() = {

        val lstQT = ListBuffer(this)

        lstQT.map(qTree => {

            if (qTree.topLeft != null) lstQT.append(qTree.topLeft)
            if (qTree.topRight != null) lstQT.append(qTree.topRight)
            if (qTree.bottomLeft != null) lstQT.append(qTree.bottomLeft)
            if (qTree.bottomRight != null) lstQT.append(qTree.bottomRight)
        })

        lstQT.map(_.points)

        //        var idx = 0
        //        while (idx < lstQT.size) {
        //
        //            qTree = lstQT(idx)
        //
        //            if (qTree.topLeft != null) lstQT.append(qTree.topLeft)
        //            if (qTree.topRight != null) lstQT.append(qTree.topRight)
        //            if (qTree.bottomLeft != null) lstQT.append(qTree.bottomLeft)
        //            if (qTree.bottomRight != null) lstQT.append(qTree.bottomRight)
        //
        //            idx += 1
        //        }
        //
        //        lstQT.map(_.points) //.flatMap(_.seq)
    }

    def queryPoint(point: Point) = {

        //        queryPoint(this, point)
        val lstQT = ListBuffer(this)
        //        var qTree = this
        var bestQTree: QuadTree = null

        lstQT.foreach(qTree => {

            if (qTree.boundary.containsPoint(point)) {

                bestQTree = qTree

                if (qTree.topLeft != null) lstQT.append(qTree.topLeft)
                if (qTree.topRight != null) lstQT.append(qTree.topRight)
                if (qTree.bottomLeft != null) lstQT.append(qTree.bottomLeft)
                if (qTree.bottomRight != null) lstQT.append(qTree.bottomRight)
            }
        })

        //        var idx = 0
        //        while (idx < lstQT.size /* && lstQT(idx).totalPoints > pointLimit*/ ) {
        //
        //            qTree = lstQT(idx)
        //
        //            //            if (qTree.boundary.center.x.toInt.toString.equals("1533361") && qTree.boundary.center.y.toInt.toString.equals("207366")
        //            //                && searchRegion.center.x.toInt.toString.equals("1533861") && searchRegion.center.y.toInt.toString.equals("205402"))
        //            //                println
        //            //            if (idx == 17) {
        //            //
        //            //                val l = qTree.boundary.left
        //            //                val r = qTree.boundary.right
        //            //                val t = qTree.boundary.top
        //            //                val b = qTree.boundary.bottom
        //            //
        //            //                val aa = point.x >= l
        //            //                val bb = point.x <= r
        //            //                val cc = point.y >= b
        //            //                val dd = point.y <= t
        //            //
        //            //                println(aa && bb && cc && dd)
        //            //            }
        //
        //            if (qTree.boundary.containsPoint(point)) {
        //
        //                bestQTree = qTree
        //
        //                if (qTree.topLeft != null) lstQT.append(qTree.topLeft)
        //                if (qTree.topRight != null) lstQT.append(qTree.topRight)
        //                if (qTree.bottomLeft != null) lstQT.append(qTree.bottomLeft)
        //                if (qTree.bottomRight != null) lstQT.append(qTree.bottomRight)
        //            }
        //
        //            idx += 1
        //        }

        bestQTree
    }

    //    private def queryPoint(qTree: QuadTree, point: Point): QuadTree = {
    //
    //        var bestQT: QuadTree = null
    //
    //        if (qTree.boundary.containsPoint(point)) {
    //
    //            bestQT = qTree
    //
    //            val nextQT = List(topLeft, topRight, bottomLeft, bottomRight)
    //                .filter(_ != null)
    //                .filter(_.boundary.containsPoint(point))
    //
    //            if (nextQT.size > 0) {
    //
    //                val qt = queryPoint(nextQT.head, point)
    //
    //                if (qt != null)
    //                    bestQT = qt
    //            }
    //        }
    //
    //        bestQT
    //    }

    def queryWithin(range: Box): MutableList[Point] = {

        val matches = MutableList[Point]()
        queryWithin(range, matches)
        matches
    }

    private def queryWithin(queryBox: Box, matches: MutableList[Point]): Unit = {
        if (!boundary.intersects(queryBox))
            return

        points.filter(queryBox.containsPoint(_))
            .foreach(matches += _)

        if (!isLeaf) {
            if (topLeft != null) topLeft.queryWithin(queryBox, matches)
            if (topRight != null) topRight.queryWithin(queryBox, matches)
            if (bottomLeft != null) bottomLeft.queryWithin(queryBox, matches)
            if (bottomRight != null) bottomRight.queryWithin(queryBox, matches)
        }
    }

    def isLeaf = !(topLeft != null || topRight != null || bottomLeft != null || bottomRight != null)

    //def computeSubRegions(maxPointCount: Long) {
    //
    //        var lstSegments = ListBuffer[(Long, Box)]()
    //        var qtCenter = queryPoint(boundary.center)
    //
    //        val quadTreeMBR = (this.boundary.left, this.boundary.bottom, this.boundary.right, this.boundary.top)
    //
    //        val adjustedMaxPointCount = math.min(maxPointCount, this.totalPoints)
    //
    //        var searchBox = Box(this.boundary.center, qtCenter.boundary.halfDimension)
    //
    //        while (!searchBox.containsBox(this.boundary)) {
    //
    //            var foundPoints = 0L
    //
    //            while (foundPoints < adjustedMaxPointCount) {
    //
    //                searchBox = Box(this.boundary.center, Point(searchBox.halfDimension.x * 1.3, searchBox.halfDimension.y * 1.3))
    //
    //                val lstQT = ListBuffer(this)
    //
    //                lstQT.foreach(qt => {
    //
    //                    if (qt.pointsMBR == null)
    //                        qt.pointsMBR = qt.qTreeAddendum.pointsMBR()
    //
    //                    if (!qt.qTreeAddendum.marker && qt.pointsMBR.intersects(searchBox)) {
    //
    //                        qt.qTreeAddendum.marker = true
    //                        foundPoints += qt.points.size
    //                    }
    //
    //                    if (qt.topLeft != null && qt.topLeft.boundary.intersects(searchBox))
    //                        lstQT.append(qt.topLeft)
    //                    if (qt.topRight != null && qt.topRight.boundary.intersects(searchBox))
    //                        lstQT.append(qt.topRight)
    //                    if (qt.bottomLeft != null && qt.bottomLeft.boundary.intersects(searchBox))
    //                        lstQT.append(qt.bottomLeft)
    //                    if (qt.bottomRight != null && qt.bottomRight.boundary.intersects(searchBox))
    //                        lstQT.append(qt.bottomRight)
    //                })
    //            }
    //
    //            lstSegments.append((foundPoints, searchBox))
    //        }
    //
    //        arrSegments = lstSegments.toArray
    //    }

    override def toString() =
        "%d\t%s\t%d\t%d".format(assignedPart, boundary, if (qTreeAddendum == null) -1 else points.size, totalPoints)
}

//object QuadTree {
//
//    val capacity = 4
//
//    val unused: QuadTree = null
//}
