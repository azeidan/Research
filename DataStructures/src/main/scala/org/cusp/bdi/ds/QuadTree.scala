package org.cusp.bdi.ds

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.cusp.bdi.ds.QuadTree.{SER_MARKER, SER_MARKER_NULL, quadCapacity}
import org.cusp.bdi.ds.SpatialIndex.{KnnLookupInfo, testAndAddPoint}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object QuadTree extends Serializable {

  val quadCapacity = 4

  val SER_MARKER_NULL: Byte = Byte.MinValue
  val SER_MARKER: Byte = Byte.MaxValue

  //  def intersects(quadTree: QuadTree, searchRegion: Rectangle): Boolean =
  //    quadTree != null && quadTree.rectBounds.intersects(searchRegion)
}

class QuadTree(_rectBounds: Rectangle) extends SpatialIndex {

  var totalPoints = 0
  var lstPoints: ListBuffer[Point] = ListBuffer[Point]()

  var rectBounds: Rectangle = _rectBounds
  var topLeft: QuadTree = _
  var topRight: QuadTree = _
  var bottomLeft: QuadTree = _
  var bottomRight: QuadTree = _

  override def getTotalPoints: Int = totalPoints

  override def findExact(searchXY: (Double, Double)): Point = {

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

  override def insert(iterPoints: Iterator[Point]): Boolean = {

    if (iterPoints.isEmpty)
      throw new IllegalStateException("Empty point iterator")

    iterPoints.foreach(insertPoint)

    true
  }

  private def contains(quadTree: QuadTree, searchXY: (Double, Double)) =
    quadTree != null && quadTree.rectBounds.contains(searchXY._1, searchXY._2)

  private def insertPoint(point: Point): Boolean = {

    var qTree = this

    if (this.rectBounds.contains(point))
      while (true) {

        qTree.totalPoints += 1

        if (qTree.lstPoints.size < quadCapacity) {

          qTree.lstPoints += point
          return true
        }
        else {
          // switch to proper quadrant?

          qTree = if (point.x <= qTree.rectBounds.center.x)
            if (point.y >= qTree.rectBounds.center.y) {

              if (qTree.topLeft == null)
                qTree.topLeft = new QuadTree(qTree.rectBounds.topLeftQuadrant /*, qTree*/)

              qTree.topLeft
            }
            else {

              if (qTree.bottomLeft == null)
                qTree.bottomLeft = new QuadTree(qTree.rectBounds.bottomLeftQuadrant /*, qTree*/)

              qTree.bottomLeft
            }
          else if (point.y >= qTree.rectBounds.center.y) {

            if (qTree.topRight == null)
              qTree.topRight = new QuadTree(qTree.rectBounds.topRightQuadrant /*, qTree*/)

            qTree.topRight
          }
          else {

            if (qTree.bottomRight == null)
              qTree.bottomRight = new QuadTree(qTree.rectBounds.bottomRightQuadrant /*, qTree*/)

            qTree.bottomRight
          }
        }
      }

    throw new Exception("Point insert failed: %s in QuadTree: %s".format(point, this))
  }

  override def toString: String =
    "%s\t%,d\t%,d".format(rectBounds, lstPoints.size, totalPoints)

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
          kryo.writeClassAndObject(output, qTree.rectBounds)
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
      qTree.rectBounds = kryo.readClassAndObject(input) match {
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

    val sPtBestQT = findBestQuadrant(searchPoint, sortSetSqDist.maxSize)

    //    val dim = if (sortSetSqDist.isFull)
    //      math.sqrt(sortSetSqDist.last.distance)
    //    else
    //      computeDimension(searchPoint, sPtBestQT.rectBounds)
    //
    //    val rectSearchRegion = Rectangle(searchPoint, new Geom2D(dim))
    //
    //    var prevMaxSqrDist = if (sortSetSqDist.last == null) -1 else sortSetSqDist.last.distance

    val knnLookupInfo = KnnLookupInfo(searchPoint, sortSetSqDist, sPtBestQT.rectBounds)

    def process(rootQT: QuadTree, skipQT: QuadTree) {

      val lstQT = ListBuffer(rootQT)

      lstQT.foreach(qTree =>
        if (qTree != skipQT)
          if (knnLookupInfo.rectSearchRegion.intersects(qTree.rectBounds)) {

            qTree.lstPoints.foreach(testAndAddPoint(_, knnLookupInfo))

            if (qTree.topLeft != null)
              lstQT += qTree.topLeft
            if (qTree.topRight != null)
              lstQT += qTree.topRight
            if (qTree.bottomLeft != null)
              lstQT += qTree.bottomLeft
            if (qTree.bottomRight != null)
              lstQT += qTree.bottomRight
          }
      )
    }

    process(sPtBestQT, null)

    if (sPtBestQT != this)
      process(this, sPtBestQT)
  }

  def findBestQuadrant(searchPoint: Geom2D, minCount: Int): QuadTree = {

    // find leaf containing point
    var qTree: QuadTree = this

    def testQuad(qtQuad: QuadTree) =
      qtQuad != null && qtQuad.totalPoints >= minCount && qtQuad.rectBounds.contains(searchPoint)

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
