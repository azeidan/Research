package org.cusp.bdi.ds.qt

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.qt.QuadTree.capacity
import org.cusp.bdi.ds.{Box, Point}

import scala.collection.mutable.ListBuffer

object QuadTree extends Serializable {

  val capacity = 4
}

class QuadTree extends KryoSerializable {

  private var totalPoints = 0L
  private var points = ListBuffer[Point]()

  //  var depth = 0L

  var boundary: Box = _
  var topLeft: QuadTree = _
  var topRight: QuadTree = _
  var bottomLeft: QuadTree = _
  var bottomRight: QuadTree = _

  def this(boundary: Box) = {
    this()
    this.boundary = boundary
  }

  override def write(kryo: Kryo, output: Output): Unit = {

    val lstQT = ListBuffer(this)

    def testAndWrite(qTree: QuadTree): Unit = {
      if (qTree == null)
        output.writeByte(Byte.MinValue)
      else {

        output.writeByte(Byte.MaxValue)
        lstQT += qTree
      }
    }

    lstQT.foreach(qTree => {

      output.writeLong(qTree.totalPoints)
      kryo.writeClassAndObject(output, qTree.boundary)
      kryo.writeClassAndObject(output, qTree.points)
      //      output.writeInt(qTree.points.size)
      //      qTree.points.foreach(pt => kryo.writeClassAndObject(output, pt))

      testAndWrite(qTree.topLeft)
      testAndWrite(qTree.topRight)
      testAndWrite(qTree.bottomLeft)
      testAndWrite(qTree.bottomRight)
    })
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    val lstQT = ListBuffer(this)

    lstQT.foreach(qTree => {

      qTree.totalPoints = input.readLong()
      qTree.boundary = kryo.readClassAndObject(input) match {
        case bx: Box => bx
      }

      qTree.points = kryo.readClassAndObject(input).asInstanceOf[ListBuffer[Point]]

      if (input.readByte() == Byte.MaxValue) {

        qTree.topLeft = new QuadTree
        lstQT += qTree.topLeft
      }
      if (input.readByte() == Byte.MaxValue) {

        qTree.topRight = new QuadTree
        lstQT += qTree.topRight
      }
      if (input.readByte() == Byte.MaxValue) {

        qTree.bottomLeft = new QuadTree
        lstQT += qTree.bottomLeft
      }
      if (input.readByte() == Byte.MaxValue) {

        qTree.bottomRight = new QuadTree
        lstQT += qTree.bottomRight
      }
    })
  }


  def getTotalPoints: Long = totalPoints

  def getLstPoint: ListBuffer[Point] = points

  def findExact(searchXY: (Double, Double)): Point = {

    var qTree = this

    while (qTree != null) {

      val lst = qTree.getLstPoint.filter(qtPoint => searchXY._1.equals(qtPoint.x) && searchXY._2.equals(qtPoint.y)).take(1)

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

  def insert(point: Point): Boolean = {
    if (!insertPoint(point))
      throw new Exception("Point insert failed: %s in QuadTree: %s".format(point, this))

    true
  }

  private def contains(quadTree: QuadTree, searchXY: (Double, Double)) =
    quadTree != null && quadTree.boundary.contains(searchXY._1, searchXY._2)

  private def insertPoint(point: Point): Boolean = {

    var qTree = this
    //    var depthCounter = 0L

    if (this.boundary.contains(point))
      while (true) {

        qTree.totalPoints += 1

        if (qTree.points.size < capacity) {

          //          if (depth < 1e3)
          qTree.points += point
          return true
        }
        else {
          // switch to proper quadrant?

          //          depthCounter += 1
          //          if (depth < depthCounter) depth = depthCounter

          qTree = if (point.x <= qTree.boundary.pointCenter.x)
            if (point.y >= qTree.boundary.pointCenter.y) {

              if (qTree.topLeft == null)
                qTree.topLeft = new QuadTree(qTree.boundary.topLeftQuadrant /*, qTree*/)

              qTree.topLeft
            }
            else {

              if (qTree.bottomLeft == null)
                qTree.bottomLeft = new QuadTree(qTree.boundary.bottomLeftQuadrant /*, qTree*/)

              qTree.bottomLeft
            }
          else if (point.y >= qTree.boundary.pointCenter.y) {

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

    false
  }

  def getAllPoints: ListBuffer[ListBuffer[Point]] = {

    val lstQT = ListBuffer(this)

    lstQT.map(qTree => {

      if (qTree.topLeft != null) lstQT += qTree.topLeft
      if (qTree.topRight != null) lstQT += qTree.topRight
      if (qTree.bottomLeft != null) lstQT += qTree.bottomLeft
      if (qTree.bottomRight != null) lstQT += qTree.bottomRight
    })

    lstQT.map(_.points) //.flatMap(_.seq)
  }

  override def toString: String =
    "%s\t%d\t%d".format(boundary, points.size, totalPoints)
}
