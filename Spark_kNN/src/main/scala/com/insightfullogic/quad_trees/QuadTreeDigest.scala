package com.insightfullogic.quad_trees

import com.insightfullogic.quad_trees.QuadTree.capacity

import scala.collection.mutable.ListBuffer

object QuadTreeDigest extends Serializable {

    val capacity = 4
}

case class QuadTreeDigest(boundary: Box) extends Serializable {

    private var pointCount = 0L
    private var totalPointWeight = 0L
    private val lstPoint = ListBuffer[Point]()
    var topLeft: QuadTreeDigest = _
    var topRight: QuadTreeDigest = _
    var bottomLeft: QuadTreeDigest = _
    var bottomRight: QuadTreeDigest = _

    var parent: QuadTreeDigest = _

    def getTotalPointWeight: Long = totalPointWeight
    def getPointCount: Long = pointCount
    def getLstPoint: ListBuffer[Point] = lstPoint

    def this(boundary: Box, parent: QuadTreeDigest) = {

        this(boundary)
        this.parent = parent
    }

    def insert(pointXY: (Double, Double), weight: Long, setQTUId: Set[Int]): Boolean = {

        val point = new Point(pointXY, (weight, setQTUId))

        if (!this.boundary.contains(point) ||
            !insertPoint(point, weight)) {

            //            insertPoint(point, weight)

            throw new Exception("Point insert failed: %s in QuadTreeDigest: %s".format(point, this))
        }

        true
    }

    private def insertPoint(point: Point, weight: Long): Boolean = {

        var qtd = this

        //        while (qtd.boundary.contains(point))
        while (true) {

            qtd.totalPointWeight += weight

            if (qtd.pointCount < capacity) {

                //                if (point.x.toString().startsWith("15218") && point.y.toString().startsWith("3360"))
                //                    println(qtd.boundary.contains(point))

                qtd.pointCount += 1

                qtd.lstPoint.append(point)

                return true
            }
            else
                // switch to proper quadrant?
                qtd = if (point.x <= qtd.boundary.center.x)
                    if (point.y >= qtd.boundary.center.y) {

                        if (qtd.topLeft == null)
                            qtd.topLeft = new QuadTreeDigest(qtd.boundary.topLeftQuadrant, qtd)

                        qtd.topLeft
                    }
                    else {

                        if (qtd.bottomLeft == null)
                            qtd.bottomLeft = new QuadTreeDigest(qtd.boundary.bottomLeftQuadrant, qtd)

                        qtd.bottomLeft
                    }
                else if (point.y >= qtd.boundary.center.y) {

                    if (qtd.topRight == null)
                        qtd.topRight = new QuadTreeDigest(qtd.boundary.topRightQuadrant, qtd)

                    qtd.topRight
                }
                else {

                    if (qtd.bottomRight == null)
                        qtd.bottomRight = new QuadTreeDigest(qtd.boundary.bottomRightQuadrant, qtd)

                    qtd.bottomRight
                }
        }

        false
    }

    override def toString: String =
        "%s\t%d\t%d".format(boundary, pointCount, totalPointWeight)
}
