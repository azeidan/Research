package org.cusp.bdi.ds

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.cusp.bdi.ds.QuadTree.{SER_MARKER, SER_MARKER_NULL, intersects, quadCapacity}
import org.cusp.bdi.ds.SpatialIndex.testAndAddPoint
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object QuadTree extends Serializable {

  val quadCapacity = 4

  val SER_MARKER_NULL: Byte = Byte.MinValue
  val SER_MARKER: Byte = Byte.MaxValue

  def intersects(quadTree: QuadTree, searchRegion: Rectangle): Boolean =
    quadTree != null && searchRegion.intersects(quadTree.boundary)
}

class QuadTree(_boundary: Rectangle) extends SpatialIndex {

  var totalPoints = 0
  var lstPoints: ListBuffer[Point] = ListBuffer[Point]()

  var boundary: Rectangle = _boundary
  var topLeft: QuadTree = _
  var topRight: QuadTree = _
  var bottomLeft: QuadTree = _
  var bottomRight: QuadTree = _

  override def getTotalPoints: Int = totalPoints

  def findExact(searchXY: (Double, Double)): Point = {

    var qTree = this

    while (qTree != null) {

      val lst = qTree.lstPoints.filter(qtPoint => searchXY._1.equals(qtPoint.x) && searchXY._2.equals(qtPoint.y)).take(1)

      if (lst.isEmpty)
        if (contains(qTree.topLeft, searchXY))
          qTree = qTree.topLeft
        else if (contains(qTree.topRight, searchXY))
          qTree = qTree.topRight
        else if (contains(qTree.bottomLeft, searchXY))
          qTree = qTree.bottomLeft
        else if (contains(qTree.bottomRight, searchXY))
          qTree = qTree.bottomRight
        else
          qTree = null
      else
        return lst.head
    }

    null
  }

  def insert(iterPoints: Iterator[Point]): Boolean = {

    iterPoints.foreach(insertPoint)

    true
  }

  private def contains(quadTree: QuadTree, searchXY: (Double, Double)) =
    quadTree != null && quadTree.boundary.contains(searchXY._1, searchXY._2)

  private def insertPoint(point: Point): Boolean = {

    var qTree = this

    if (this.boundary.contains(point))
      while (true) {

        qTree.totalPoints += 1

        if (qTree.lstPoints.size < quadCapacity) {

          qTree.lstPoints += point
          return true
        }
        else {
          // switch to proper quadrant?

          qTree = if (point.x <= qTree.boundary.center.x)
            if (point.y >= qTree.boundary.center.y) {

              if (qTree.topLeft == null)
                qTree.topLeft = new QuadTree(qTree.boundary.topLeftQuadrant /*, qTree*/)

              qTree.topLeft
            }
            else {

              if (qTree.bottomLeft == null)
                qTree.bottomLeft = new QuadTree(qTree.boundary.bottomLeftQuadrant /*, qTree*/)

              qTree.bottomLeft
            }
          else if (point.y >= qTree.boundary.center.y) {

            if (qTree.topRight == null)
              qTree.topRight = new QuadTree(qTree.boundary.topRightQuadrant /*, qTree*/)

            qTree.topRight
          }
          else {

            if (qTree.bottomRight == null)
              qTree.bottomRight = new QuadTree(qTree.boundary.bottomRightQuadrant /*, qTree*/)

            qTree.bottomRight
          }
        }
      }

    throw new Exception("Point insert failed: %s in QuadTree: %s".format(point, this))
  }

  //  def getAllPoints: ListBuffer[ListBuffer[Point]] = {
  //
  //    val lstQT = ListBuffer(this)
  //
  //    lstQT.map(qTree => {
  //
  //      if (qTree.topLeft != null) lstQT += qTree.topLeft
  //      if (qTree.topRight != null) lstQT += qTree.topRight
  //      if (qTree.bottomLeft != null) lstQT += qTree.bottomLeft
  //      if (qTree.bottomRight != null) lstQT += qTree.bottomRight
  //    })
  //
  //    lstQT.map(_.lstPoints)
  //  }

  override def toString: String =
    "%s\t%d\t%d".format(boundary, lstPoints.size, totalPoints)

  override def write(kryo: Kryo, output: Output): Unit = {

    val queueQT = mutable.Queue(this)

    while (queueQT.nonEmpty) {

      val qTree = queueQT.dequeue()

      qTree match {
        case null =>
          output.writeByte(SER_MARKER_NULL)
        case _ =>

          output.writeByte(SER_MARKER)
          output.writeLong(qTree.totalPoints)
          kryo.writeClassAndObject(output, qTree.boundary)
          kryo.writeClassAndObject(output, qTree.lstPoints)

          queueQT += (qTree.topLeft, qTree.topRight, qTree.bottomLeft, qTree.bottomRight)
      }
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    def instantiateQT() =
      input.readByte() match {
        case SER_MARKER_NULL => null
        case _ => new QuadTree(null)
      }

    //    instantiateQT() // gets rid of the root QT marker

    val queueQT = mutable.Queue(this)

    while (queueQT.nonEmpty) {

      val qTree = queueQT.dequeue()

      qTree.totalPoints = input.readInt()
      qTree.boundary = kryo.readClassAndObject(input) match {
        case bx: Rectangle => bx
      }

      qTree.lstPoints = kryo.readClassAndObject(input).asInstanceOf[ListBuffer[Point]]

      qTree.topLeft = instantiateQT()
      qTree.topRight = instantiateQT()
      qTree.bottomLeft = instantiateQT()
      qTree.bottomRight = instantiateQT()

      queueQT += (qTree.topLeft, qTree.topRight, qTree.bottomLeft, qTree.bottomRight)
    }
  }

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedList[Point]) {

    //    if (searchPoint.userData.toString().equalsIgnoreCase("yellow_3_a_772558"))
    //      println

    val sPtBestQT: QuadTree = getBestQuadrant(searchPoint, sortSetSqDist.maxSize)

    val dim = if (sortSetSqDist.isFull)
      math.sqrt(sortSetSqDist.last.distance)
    else
      math.max(math.max(math.abs(searchPoint.x - sPtBestQT.boundary.left), math.abs(searchPoint.x - sPtBestQT.boundary.right)),
        math.max(math.abs(searchPoint.y - sPtBestQT.boundary.bottom), math.abs(searchPoint.y - sPtBestQT.boundary.top)))

    val rectSearchRegion = Rectangle(searchPoint, new Geom2D(dim, dim))

    pointsWithinRegion(sPtBestQT, rectSearchRegion, sortSetSqDist)
  }

  private def pointsWithinRegion(sPtBestQT: QuadTree, rectSearchRegion: Rectangle, sortSetSqDist: SortedList[Point]) {

    var prevMaxSqrDist = if (sortSetSqDist.last == null) -1 else sortSetSqDist.last.distance

    def process(rootQT: QuadTree, skipQT: QuadTree) {

      val lstQT = ListBuffer(rootQT)

      lstQT.foreach(qTree =>
        if (qTree != skipQT) {

          qTree.lstPoints.foreach(pt => prevMaxSqrDist = testAndAddPoint(pt, rectSearchRegion, sortSetSqDist, prevMaxSqrDist))

          if (intersects(qTree.topLeft, rectSearchRegion))
            lstQT += qTree.topLeft
          if (intersects(qTree.topRight, rectSearchRegion))
            lstQT += qTree.topRight
          if (intersects(qTree.bottomLeft, rectSearchRegion))
            lstQT += qTree.bottomLeft
          if (intersects(qTree.bottomRight, rectSearchRegion))
            lstQT += qTree.bottomRight
        }
      )
    }

    if (sPtBestQT != null)
      process(sPtBestQT, null)

    if (sPtBestQT != this)
      process(this, sPtBestQT)
  }

  def getBestQuadrant(searchPoint: Geom2D, minCount: Int): QuadTree = {

    // find leaf containing point
    var qTree: QuadTree = this

    def testQuad(qtQuad: QuadTree) =
      qtQuad != null && qtQuad.totalPoints >= minCount && qtQuad.boundary.contains(searchPoint)

    while (true)
      if (testQuad(qTree.topLeft))
        qTree = qTree.topLeft
      else if (testQuad(qTree.topRight))
        qTree = qTree.topRight
      else if (testQuad(qTree.bottomLeft))
        qTree = qTree.bottomLeft
      else if (testQuad(qTree.bottomRight))
        qTree = qTree.bottomRight
      else
        return qTree

    null
  }
}