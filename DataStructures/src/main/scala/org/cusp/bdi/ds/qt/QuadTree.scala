package org.cusp.bdi.ds.qt

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import org.cusp.bdi.ds.SpatialIndex
import org.cusp.bdi.ds.SpatialIndex.{KnnLookupInfo, testAndAddPoint}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.qt.QuadTree.{SER_MARKER_NULL, nodeCapacity}
import org.cusp.bdi.ds.sortset.SortedLinkedList

import scala.collection.mutable.ArrayBuffer
import scala.collection.{AbstractIterator, mutable}

object QuadTree extends Serializable {

  val nodeCapacity = 4

  val SER_MARKER_NULL: Byte = Byte.MinValue
}

class QuadTree extends SpatialIndex {

  var totalPoints = 0
  var arrPoints = ArrayBuffer[Point]()

  var rectBounds: Rectangle = _
  var topLeft: QuadTree = _
  var topRight: QuadTree = _
  var bottomLeft: QuadTree = _
  var bottomRight: QuadTree = _

  private def this(rectBounds: Rectangle) = {
    this()
    this.rectBounds = rectBounds
  }

  override def mockNode: AnyRef =
    new QuadTree()

  override def estimateNodeCount(objCount: Long): Int =
    (math.floor((objCount - nodeCapacity) / 7.0 * 4) + 1).toInt

  //    math.ceil(pointCount / nodeCapacity.toFloat).toInt

  override def getTotalPoints: Int = totalPoints

  override def findExact(searchXY: (Double, Double)): Point = {

    //    if(searchXY._1.toInt==15407 && searchXY._2.toInt==3243)
    //      println

    var qTree = this

    while (qTree != null) {

      val optPoint = qTree.arrPoints.find(qtPoint => searchXY._1.equals(qtPoint.x) && searchXY._2.equals(qtPoint.y))

      if (optPoint.isEmpty)
        qTree =
          if (contains(qTree.topLeft, searchXY)) qTree.topLeft
          else if (contains(qTree.topRight, searchXY)) qTree.topRight
          else if (contains(qTree.bottomLeft, searchXY)) qTree.bottomLeft
          else if (contains(qTree.bottomRight, searchXY)) qTree.bottomRight
          else null
      else
        return optPoint.get
    }

    null
  }

  @throws(classOf[IllegalStateException])
  override def insert(rectBounds: Rectangle, iterPoints: Iterator[Point], histogramBarWidth: Int) {

    if (iterPoints.isEmpty) throw new IllegalStateException("Empty point iterator")
    if (rectBounds == null) throw new IllegalStateException("Rectangle bounds cannot be null")

    this.rectBounds = rectBounds

    iterPoints.foreach(insertPoint)
  }

  private def contains(quadTree: QuadTree, searchXY: (Double, Double)) =
    quadTree != null && quadTree.rectBounds.contains(searchXY)

  private def insertPoint(point: Point): Boolean = {

    //    if (point.userData.toString.equalsIgnoreCase("Yellow_2_A_507601"))
    //      println()

    var qTree = this

    if (this.rectBounds.contains(point))
      while (true) {

        qTree.totalPoints += 1

        if (qTree.arrPoints.length < nodeCapacity) {

          qTree.arrPoints += point
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
    "%s\t%,d\t%,d".format(rectBounds, arrPoints.length, totalPoints)

  override def write(kryo: Kryo, output: Output): Unit = {

    val queueQT = mutable.Queue(this)

    def writeQT(qt: QuadTree) =
      output.writeByte(if (qt == null)
        SER_MARKER_NULL
      else {

        queueQT += qt

        Byte.MaxValue
      })

    while (queueQT.nonEmpty) {

      val qTree = queueQT.dequeue()

      output.writeInt(qTree.totalPoints)

      output.writeInt(qTree.arrPoints.length)
      qTree.arrPoints.foreach(kryo.writeObject(output, _))

      kryo.writeObject(output, qTree.rectBounds)

      writeQT(qTree.topLeft)
      writeQT(qTree.topRight)
      writeQT(qTree.bottomLeft)
      writeQT(qTree.bottomRight)
    }
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    def instantiateQT() =
      input.readByte() match {
        case SER_MARKER_NULL => null
        case _ => new QuadTree(null)
      }

    val qQT = mutable.Queue(this)

    while (qQT.nonEmpty) {

      val qTree = qQT.dequeue()

      qTree.totalPoints = input.readInt()

      val arrLength = input.readInt()

      qTree.arrPoints = ArrayBuffer[Point]()
      qTree.arrPoints.sizeHint(arrLength)

      (0 until arrLength).foreach(_ => qTree.arrPoints += kryo.readObject(input, classOf[Point]))

      qTree.rectBounds = kryo.readObject(input, classOf[Rectangle])

      qTree.topLeft = instantiateQT()
      qTree.topRight = instantiateQT()
      qTree.bottomLeft = instantiateQT()
      qTree.bottomRight = instantiateQT()

      if (qTree.topLeft != null) qQT += qTree.topLeft
      if (qTree.topRight != null) qQT += qTree.topRight
      if (qTree.bottomLeft != null) qQT += qTree.bottomLeft
      if (qTree.bottomRight != null) qQT += qTree.bottomRight
    }
  }

  override def nearestNeighbor(searchPoint: Point, sortSetSqDist: SortedLinkedList[Point]) {

    val sPtBestQT = findBestQuadrant(searchPoint, sortSetSqDist.capacity)

    val knnLookupInfo = new KnnLookupInfo(searchPoint, sortSetSqDist, sPtBestQT.rectBounds)

    def process(rootQT: QuadTree, skipQT: QuadTree) {

      val queueQT = mutable.Queue(rootQT)

      while (queueQT.nonEmpty) {

        val qTree = queueQT.dequeue()

        if (qTree != skipQT && knnLookupInfo.rectSearchRegion.intersects(qTree.rectBounds)) {

          qTree.arrPoints.foreach(testAndAddPoint(_, knnLookupInfo))

          if (qTree.topLeft != null)
            queueQT += qTree.topLeft
          if (qTree.topRight != null)
            queueQT += qTree.topRight
          if (qTree.bottomLeft != null)
            queueQT += qTree.bottomLeft
          if (qTree.bottomRight != null)
            queueQT += qTree.bottomRight
        }
      }
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

  override def allPoints: Iterator[ArrayBuffer[Point]] = new AbstractIterator[ArrayBuffer[Point]] {

    private val queue = mutable.Queue[QuadTree](QuadTree.this)

    override def hasNext: Boolean = queue.nonEmpty

    override def next(): ArrayBuffer[Point] =
      if (!hasNext)
        throw new NoSuchElementException("next on empty Iterator")
      else {

        val ans = queue.dequeue()

        if (ans.topLeft != null) queue += ans.topLeft
        if (ans.topRight != null) queue += ans.topRight
        if (ans.bottomLeft != null) queue += ans.bottomLeft
        if (ans.bottomRight != null) queue += ans.bottomRight

        ans.arrPoints
      }
  }
}
