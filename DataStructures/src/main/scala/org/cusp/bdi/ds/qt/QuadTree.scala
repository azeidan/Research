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
    math.ceil(((objCount - nodeCapacity) / 7.0 * 4) + 1).toInt

  override def estimateObjCount(gIdxNodeCount: Int): Long = -1

  override def getTotalPoints: Int = totalPoints

  override def findExact(searchXY: (Double, Double)): Point = {

    //    if(searchXY._1.toInt==15407 && searchXY._2.toInt==3243)
    //      println

    def contains(quadTree: QuadTree) =
      quadTree != null && quadTree.rectBounds.contains(searchXY)

    var qTree = this

    while (qTree != null) {

      val optPoint = qTree.arrPoints.find(qtPoint => searchXY._1.equals(qtPoint.x) && searchXY._2.equals(qtPoint.y))

      if (optPoint.isEmpty)
        qTree =
          getNextQuad(qTree, searchXY._1, searchXY._2) match {
            case 0 => if (contains(qTree.bottomLeft)) qTree.bottomLeft else null
            case 1 => if (contains(qTree.topLeft)) qTree.topLeft else null
            case 2 => if (contains(qTree.bottomRight)) qTree.bottomRight else null
            case _ => if (contains(qTree.topRight)) qTree.topRight else null
          }
      else
        return optPoint.get
    }

    null
  }

  @throws(classOf[IllegalStateException])
  override def insert(rectBounds: Rectangle, iterPoints: Iterator[Point], histogramBarWidth: Int) {

    //    if (iterPoints.isEmpty) throw new IllegalStateException("Empty point iterator")
    if (rectBounds == null) throw new IllegalStateException("Rectangle bounds cannot be null")

    this.rectBounds = rectBounds

    iterPoints.foreach(insertPoint)
  }

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
          qTree = getNextQuad(qTree, point.x, point.y) match {
            case 0 =>
              if (qTree.bottomLeft == null)
                qTree.bottomLeft = new QuadTree(qTree.rectBounds.bottomLeftQuadrant)

              qTree.bottomLeft
            case 1 =>
              if (qTree.topLeft == null)
                qTree.topLeft = new QuadTree(qTree.rectBounds.topLeftQuadrant)

              qTree.topLeft
            case 2 =>
              if (qTree.bottomRight == null)
                qTree.bottomRight = new QuadTree(qTree.rectBounds.bottomRightQuadrant)

              qTree.bottomRight
            case _ =>
              if (qTree.topRight == null)
                qTree.topRight = new QuadTree(qTree.rectBounds.topRightQuadrant)

              qTree.topRight
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

    //    if (searchPoint.userData.toString.equalsIgnoreCase("Bus_1_A_270"))
    //      println

    val knnLookupInfo = KnnLookupInfo(searchPoint, sortSetSqDist)

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

    if (sortSetSqDist.isFull)
      process(this, null)
    else {

      val sPtBestQT = findBestQuadrant(searchPoint, sortSetSqDist.capacity)

      process(sPtBestQT, null)
      process(this, sPtBestQT)
    }
  }

  def findBestQuadrant(searchPoint: Geom2D, minCount: Int): QuadTree = {

    // find leaf containing point
    var qTree: QuadTree = this

    def testQuad(qtQuad: QuadTree) =
      qtQuad != null && qtQuad.totalPoints >= minCount && qtQuad.rectBounds.contains(searchPoint)

    while (true)
      getNextQuad(qTree, searchPoint.x, searchPoint.y) match {
        case 0 => if (testQuad(qTree.bottomLeft)) qTree = qTree.bottomLeft else return qTree
        case 1 => if (testQuad(qTree.topLeft)) qTree = qTree.topLeft else return qTree
        case 2 => if (testQuad(qTree.bottomRight)) qTree = qTree.bottomRight else return qTree
        case _ => if (testQuad(qTree.topRight)) qTree = qTree.topRight else return qTree
      }

    null
  }

  private def getNextQuad(qTree: QuadTree, searchX: Double, searchY: Double) =
    if (searchX <= qTree.rectBounds.center.x) // Left
      if (searchY <= qTree.rectBounds.center.y) 0 // Bottom
      else 1 // Top
    else // Right
      if (searchY <= qTree.rectBounds.center.y) 2 // Bottom
      else 3 // Top

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
