package org.cusp.bdi.sknn

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.geom.Point
import org.cusp.bdi.ds.sortset.SortedLinkedList

import scala.collection.mutable.ArrayBuffer

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

  var left: Int = Int.MaxValue
  var bottom: Int = Int.MaxValue
  var right: Int = Int.MinValue
  var top: Int = Int.MinValue

  def this(seedX: Int, seedY: Int) = {

    this()
    this.left = seedX
    this.bottom = seedY
    this.right = seedX
    this.top = seedY
  }

  def this(seed: (Int, Int)) =
    this(seed._1, seed._2)

  def merge(other: MBRInfo): MBRInfo = {

    if (other.left < left) left = other.left
    if (other.bottom < bottom) bottom = other.bottom
    if (other.right > right) right = other.right
    if (other.top > top) top = other.top

    this
  }

  def stretch(): MBRInfo = {

    //    this.left = Math.floor(this.left).toFloat
    //    this.bottom = Math.floor(this.bottom).toFloat

    this.right += 1 // Math.ceil(this.right).toFloat
    this.top += 1 // Math.ceil(this.top).toFloat

    this
  }

  def width: Int = right - left

  def height: Int = top - bottom

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

  def nextPartId: Int =
    if (arrPartitionId.nonEmpty) {

      val pId = arrPartitionId.head

      arrPartitionId = arrPartitionId.tail

      pId
    }
    else
      -1

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

    (0 until arrLength).foreach(_ => arrPartitionId += input.readInt)
  }
}