package org.cusp.bdi.sknn

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.geom.Point
import org.cusp.bdi.ds.sortset.SortedLinkedList
import org.cusp.bdi.util.RandomWeighted

import scala.collection.mutable.ArrayBuffer

class RandomWeighted2 extends RandomWeighted with KryoSerializable {

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeFloat(totalItemWeight)
    output.writeInt(arrItemWeight.length)

    arrItemWeight.foreach(tuple => kryo.writeClassAndObject(output, tuple))
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    totalItemWeight = input.readFloat()

    val len = input.readInt()

    for (_ <- 0 until len)
      arrItemWeight += kryo.readClassAndObject(input).asInstanceOf[(Int, Float)]
  }
}

object SupportedKnnOperations extends Enumeration with Serializable {

  val knn: SupportedKnnOperations.Value = Value("knn")
  val allKnn: SupportedKnnOperations.Value = Value("allknn")
}

/*
 [@specialized(Float, Double) T: Fractional]
  val fractionalOps = implicitly[Fractional[T]]

  import fractionalOps._
 */

case class InsufficientMemoryException(message: String) extends Exception(message) {}

final class MBRInfo extends KryoSerializable with Serializable {

  var left: (Int, Int) = _
  var bottom: (Int, Int) = _
  var right: (Int, Int) = _
  var top: (Int, Int) = _

  def this(startXY: (Int, Int)) = {

    this()
    this.left = startXY
    this.bottom = startXY
    this.right = startXY
    this.top = startXY
  }

  def contains(lookupXY: (Int, Int)): Boolean =
    !(lookupXY._1 < left._1 || lookupXY._1 > right._1 || lookupXY._2 < bottom._2 || lookupXY._2 > top._2)

  def merge(other: MBRInfo): MBRInfo = {

    if (other.left._1 < left._1) left = other.left
    if (other.bottom._2 < bottom._2) bottom = other.bottom
    if (other.right._1 > right._1) right = other.right
    if (other.top._2 > top._2) top = other.top

    this
  }

  //  def stretch(): MBRInfo = {
  //
  //    //    this.left = Math.floor(this.left).toFloat
  //    //    this.bottom = Math.floor(this.bottom).toFloat
  //
  //    this.right = (this.right._1 + 1, this.right._2) // Math.ceil(this.right).toFloat
  //    this.top = (this.top._1, this.top._2 + 1) // Math.ceil(this.top).toFloat
  //
  //    this
  //  }

  def width: Int = right._1 - left._1

  def height: Int = top._2 - bottom._2

  override def toString: String =
    "%,d\t%,d\t%,d\t%,d".format(left._1, bottom._2, right._1, top._2)

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeInt(left._1)
    output.writeInt(left._2)
    output.writeInt(bottom._1)
    output.writeInt(bottom._2)
    output.writeInt(right._1)
    output.writeInt(right._2)
    output.writeInt(top._1)
    output.writeInt(top._2)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    left = (input.readInt(), input.readInt())
    bottom = (input.readInt(), input.readInt())
    right = (input.readInt(), input.readInt())
    top = (input.readInt(), input.readInt())
  }
}

final class RowData extends KryoSerializable {

  var point: Point = _
  var sortedList: SortedLinkedList[Point] = _
  var arrPartitionId: ArrayBuffer[Int] = _

  def this(point: Point, sortedList: SortedLinkedList[Point], arrPartitionId: ArrayBuffer[Int]) = {

    this()

    this.point = point
    this.sortedList = sortedList
    this.arrPartitionId = arrPartitionId
  }

  def nextPartId(default: Int): Int =
    if (arrPartitionId.nonEmpty) {

      val pId = arrPartitionId.head

      arrPartitionId = arrPartitionId.tail

      pId
    }
    else
      default

  override def write(kryo: Kryo, output: Output): Unit = {

    kryo.writeObject(output, point)
    kryo.writeObject(output, sortedList)

    output.writeInt(arrPartitionId.length)
    arrPartitionId.foreach(output.writeInt)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    point = kryo.readObject(input, classOf[Point])
    sortedList = kryo.readObject(input, classOf[SortedLinkedList[Point]])

    val arrLength = input.readInt()

    arrPartitionId = new ArrayBuffer[Int]()
    arrPartitionId.sizeHint(arrLength)

    for (_ <- 0 until arrLength)
      arrPartitionId += input.readInt
  }
}