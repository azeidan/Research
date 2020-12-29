package org.cusp.bdi.ds.kdt

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.cusp.bdi.ds.geom.{Point, Rectangle}
import org.cusp.bdi.ds.kdt.KdtNode.SPLIT_VAL_NONE

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

object KdtNode {
  val SPLIT_VAL_NONE: Double = Double.NegativeInfinity
}

final class KdtBranchRootNode extends KdtNode {

  private var _totalPoints: Int = 0
  var splitVal: Double = SPLIT_VAL_NONE
  var left: KdtNode = _
  var right: KdtNode = _

  override def totalPoints: Int = _totalPoints

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

  override def totalPoints: Int = lstPoints.length

  def this(nodeInfo: (ListBuffer[Point], Rectangle)) = {

    this()
    this.lstPoints = nodeInfo._1
    this.rectNodeBounds = nodeInfo._2
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