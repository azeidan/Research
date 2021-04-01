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

final class MBR extends KryoSerializable with Serializable {

  var left: Int = Int.MaxValue
  var bottom: Int = Int.MaxValue
  var right: Int = Int.MinValue
  var top: Int = Int.MinValue

  def this(seedXY: (Int, Int)) = {

    this()
    this.left = seedXY._1
    this.bottom = seedXY._2
    this.right = left
    this.top = bottom
  }

  def update(newGridXY: (Int, Int)): Unit = {

    right = newGridXY._1

    if (newGridXY._2 < bottom)
      bottom = newGridXY._2
    else if (newGridXY._2 > top)
      top = newGridXY._2
  }

  def merge(other: MBR): MBR = {

    if (other.left < left) left = other.left
    if (other.bottom < bottom) bottom = other.bottom
    if (other.right > right) right = other.right
    if (other.top > top) top = other.top

    this
  }

  override def toString: String =
    "%,d\t%,d\t%,d\t%,d".format(left, bottom, right, top)

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeInt(left)
    output.writeInt(bottom)
    output.writeInt(right)
    output.writeInt(top)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    left = input.readInt()
    bottom = input.readInt()
    right = input.readInt()
    top = input.readInt()
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