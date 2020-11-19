package org.cusp.bdi.ds.kdt

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.cusp.bdi.ds.geom.{Point, Rectangle}

import scala.collection.mutable.ListBuffer

abstract class KdtNode extends KryoSerializable {

  def totalPoints: Int

  var rectNodeBounds: Rectangle = _

  override def toString: String =
    "%s\t%,d".format(rectNodeBounds, totalPoints)

  override def write(kryo: Kryo, output: Output): Unit =
    kryo.writeClassAndObject(output, rectNodeBounds)

  override def read(kryo: Kryo, input: Input): Unit =
    rectNodeBounds = kryo.readClassAndObject(input) match {
      case rectangle: Rectangle => rectangle
    }
}

final class KdtBranchRootNode extends KdtNode {

  private var _totalPoints: Int = 0
  var splitVal: Double = 0
  var left: KdtNode = _
  var right: KdtNode = _

  override def totalPoints: Int = _totalPoints

  def totalPoints(totalPoints: Int) {
    this._totalPoints = totalPoints
  }

  def this(splitVal: Double, totalPoints: Int) = {

    this()
    this.splitVal = splitVal
    this._totalPoints = totalPoints
  }

  override def toString: String =
    "%s\t[%,.4f]\t%s %s".format(super.toString, splitVal, if (left == null) '-' else '/', if (right == null) '-' else '\\')

  override def write(kryo: Kryo, output: Output) {

    super.write(kryo, output)
    output.writeInt(_totalPoints)
    output.writeDouble(splitVal)
  }

  override def read(kryo: Kryo, input: Input) {

    super.read(kryo, input)
    _totalPoints = input.readInt()
    splitVal = input.readDouble()
  }
}

final class KdtLeafNode extends KdtNode {

  var lstPoints: ListBuffer[Point] = _

  override def totalPoints: Int = lstPoints.size

  def this(lstPoints: ListBuffer[Point], rectNodeBounds: Rectangle) = {

    this()
    this.lstPoints = lstPoints
    this.rectNodeBounds = rectNodeBounds
  }

  override def write(kryo: Kryo, output: Output): Unit = {

    super.write(kryo, output)

    kryo.writeClassAndObject(output, lstPoints)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    super.read(kryo, input)

    lstPoints = kryo.readClassAndObject(input).asInstanceOf[ListBuffer[Point]]
  }
}