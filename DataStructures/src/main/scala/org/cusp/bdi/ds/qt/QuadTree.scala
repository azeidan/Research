package org.cusp.bdi.ds.qt

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.cusp.bdi.ds.SpatialIndex
import org.cusp.bdi.ds.SpatialIndex.{KnnLookupInfo, testAndAddPoint}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.qt.QuadTree.{SER_MARKER, SER_MARKER_NULL, nodeCapacity}
import org.cusp.bdi.ds.sortset.SortedList
import org.cusp.bdi.util.Helper

import scala.collection.mutable.ListBuffer
import scala.collection.{AbstractIterator, mutable}

object QuadTree extends Serializable {

  val nodeCapacity = 4

  val SER_MARKER_NULL: Byte = Byte.MinValue
  val SER_MARKER: Byte = Byte.MaxValue
}

class QuadTree extends SpatialIndex {

  var totalPoints = 0
  var lstPoints: ListBuffer[Point] = ListBuffer[Point]()

  var rectBounds: Rectangle = _
  var topLeft: QuadTree = _
  var topRight: QuadTree = _
  var bottomLeft: QuadTree = _
  var bottomRight: QuadTree = _

  private def this(rectBounds: Rectangle) = {
    this()
    this.rectBounds = rectBounds
  }

  override def dummyNode: AnyRef =
    new QuadTree()

  override def estimateNodeCount(pointCount: Long): Int =
    Math.pow(2, Helper.log2(pointCount / nodeCapacity)).toInt - 1

  override def getTotalPoints: Int = totalPoints

  override def findExact(searchXY: (Double, Double)): Point = {

    //    if(searchXY._1.toInt==15407 && searchXY._2.toInt==3243)
    //      println

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

  @throws(classOf[IllegalStateException])
  override def insert(rectBounds: Rectangle, iterPoints: Iterator[Point], otherInitializers: Any*) = {

    if (iterPoints.isEmpty) throw new IllegalStateException("Empty point iterator")
    else if (rectBounds == null) throw new IllegalStateException("Rectangle bounds cannot be null")

    this.rectBounds = rectBounds

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

        if (qTree.lstPoints.length < nodeCapacity) {

          qTree.lstPoints += point
          return true
        }
        else {
          // switch to proper quadrant?

          qTree = if (point.x <= qTree.rectBounds.center.x)
            if (point.y >= qTree.rectBounds.center.y) {

              if (qTree.topLeft == null)
                qTree.topLeft = new QuadTree(qTree.rectBounds.topLeftQuadrant)

              qTree.topLeft
            }
            else {

              if (qTree.bottomLeft == null)
                qTree.bottomLeft = new QuadTree(qTree.rectBounds.bottomLeftQuadrant)

              qTree.bottomLeft
            }
          else if (point.y >= qTree.rectBounds.center.y) {

            if (qTree.topRight == null)
              qTree.topRight = new QuadTree(qTree.rectBounds.topRightQuadrant)

            qTree.topRight
          }
          else {

            if (qTree.bottomRight == null)
              qTree.bottomRight = new QuadTree(qTree.rectBounds.bottomRightQuadrant)

            qTree.bottomRight
          }
        }
      }

    throw new Exception("Point insert failed: %s in QuadTree: %s".format(point, this))
  }

  override def toString: String =
    "%s\t%,d\t%,d".format(rectBounds, lstPoints.length, totalPoints)

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

    val sPtBestQT = findBestQuadrant(searchPoint, sortSetSqDist.maxSize)

    val knnLookupInfo = new KnnLookupInfo(searchPoint, sortSetSqDist, sPtBestQT.rectBounds)

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

  override def allPoints: Iterator[Iterator[Point]] = new AbstractIterator[Iterator[Point]] {

    private val queue = mutable.Queue[QuadTree](QuadTree.this)

    override def hasNext: Boolean = queue.nonEmpty

    override def next(): Iterator[Point] =
      if (!hasNext)
        throw new NoSuchElementException("next on empty Iterator")
      else {

        val ans = queue.dequeue()

        if (ans.topLeft != null) queue += ans.topLeft
        if (ans.topRight != null) queue += ans.topRight
        if (ans.bottomLeft != null) queue += ans.bottomLeft
        if (ans.bottomRight != null) queue += ans.bottomRight

        ans.lstPoints.iterator
      }
  }
}
