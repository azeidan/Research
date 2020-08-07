/*
 package com.insightfullogic.quad_trees
 

import scala.collection.Iterable
import scala.collection.mutable.ListBuffer

import QuadTree.capacity

case class QuadTreeMBR(boundary: Box) extends Serializable {

    var assignedPart = -1
    var totalPoints = 0
    var quadPointCount = 0
    // var parent: QuadTreeMBR = null
    var topLeft: QuadTreeMBR = null
    var topRight: QuadTreeMBR = null
    var bottomLeft: QuadTreeMBR = null
    var bottomRight: QuadTreeMBR = null
    var pointsMinX: Double =
        Double.MaxValue
    var pointsMinY: Double =
        Double.MaxValue
    var pointsMaxX: Double =
        Double.MinValue
    var pointsMaxY: Double =
        Double.MinValue

    //    private var innerMBR: (Double, Double, Double, Double) = null

    //    def this(boundary: Box, parent: QuadTreeMBR) = {
    //
    //        this(boundary)
    //
    //        this.parent = parent
    //    }

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

        while (pointLeft >= qTree.boundary.left && pointRight <= qTree.boundary.right &&
            pointBottom >= qTree.boundary.bottom && pointTop <= qTree.boundary.top) {

            qTree.totalPoints += 1

            if (qTree.quadPointCount < capacity) {

                if (qTree.quadPointCount == 0) {

                    qTree.pointsMinX = point.x
                    qTree.pointsMinY = point.y
                    qTree.pointsMaxX = point.x
                    qTree.pointsMaxY = point.y
                }
                else {

                    if (point.x < qTree.pointsMinX) qTree.pointsMinX = point.x
                    else if (point.x > qTree.pointsMaxX) qTree.pointsMaxX = point.x
                    if (point.y < qTree.pointsMinY) qTree.pointsMinY = point.y
                    else if (point.y > qTree.pointsMaxY) qTree.pointsMaxY = point.y
                }

                qTree.quadPointCount += 1

                return true
            }
            else
                qTree = if (pointRight <= qTree.boundary.center.x)
                    if (pointBottom >= qTree.boundary.center.y) {
                        if (qTree.topLeft == null) qTree.topLeft = new QuadTreeMBR(qTree.boundary.topLeftQuadrant())
                        qTree.topLeft
                    }
                    else {
                        if (qTree.bottomLeft == null) qTree.bottomLeft = new QuadTreeMBR(qTree.boundary.bottomLeftQuadrant())
                        qTree.bottomLeft
                    }
                else if (pointBottom >= qTree.boundary.center.y) {
                    if (qTree.topRight == null) qTree.topRight = new QuadTreeMBR(qTree.boundary.topRightQuadrant())
                    qTree.topRight
                }
                else {
                    if (qTree.bottomRight == null) qTree.bottomRight = new QuadTreeMBR(qTree.boundary.bottomRightQuadrant())
                    qTree.bottomRight
                }
        }

        false
    }

    def findBestQuad(point: Point, k: Int) = {

        val lstQT = ListBuffer[QuadTreeMBR]()

        //        if (this.boundary.containsPoint(point)) {

        if (this.boundary.contains(point)) {

            lstQT.append(this)

            lstQT.foreach(qTree => {

                if (qTree.totalPoints >= k)
                    if (qTree.topLeft != null && qTree.topLeft.boundary.contains(point)) lstQT.append(qTree.topLeft)
                    else if (qTree.topRight != null && qTree.topRight.boundary.contains(point)) lstQT.append(qTree.topRight)
                    else if (qTree.bottomLeft != null && qTree.bottomLeft.boundary.contains(point)) lstQT.append(qTree.bottomLeft)
                    else if (qTree.bottomRight != null && qTree.bottomRight.boundary.contains(point)) lstQT.append(qTree.bottomRight)
            })
        }

        if (lstQT.isEmpty) null else lstQT(lstQT.size - 1)
    }

    def printAllPointMBRs() = {

        val lstQT = ListBuffer(this)

        lstQT.foreach(qTree => {

            println("<>\t%.8f\t%.8f\t%.8f\t%.8f".format(qTree.pointsMinX, qTree.pointsMinY, qTree.pointsMaxX, qTree.pointsMaxY))

            if (qTree.topLeft != null) lstQT.append(qTree.topLeft)
            if (qTree.topRight != null) lstQT.append(qTree.topRight)
            if (qTree.bottomLeft != null) lstQT.append(qTree.bottomLeft)
            if (qTree.bottomRight != null) lstQT.append(qTree.bottomRight)
        })
    }

    private def isLeaf = topLeft == null && topRight == null && bottomLeft == null && bottomRight == null

    // private def intersectPointMBR(quadTree: QuadTreeMBR, xyCoord: (Double, Double)) =
    // xyCoord._1 >= pointsMinX && xyCoord._1 <= pointsMaxX &&
    // xyCoord._2 >= pointsMinY && xyCoord._2 <= pointsMaxY

    override def toString() =
        "%s\t%d\t%d".format(boundary, quadPointCount, totalPoints)
}
*/