package org.cusp.bdi.sknn

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds.geom.Point
import org.cusp.bdi.ds.sortset.SortedLinkedList

import scala.collection.mutable.ListBuffer

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

  var left: Double = Double.MaxValue
  var bottom: Double = Double.MaxValue
  var right: Double = Double.MinValue
  var top: Double = Double.MinValue

  def this(seedX: Double, seedY: Double) = {

    this()
    this.left = seedX
    this.bottom = seedY
    this.right = seedX
    this.top = seedY
  }

  def this(seed: (Double, Double)) =
    this(seed._1, seed._2)

  def merge(other: MBRInfo): MBRInfo = {

    if (other.left < left) left = other.left
    if (other.bottom < bottom) bottom = other.bottom
    if (other.right > right) right = other.right
    if (other.top > top) top = other.top

    this
  }

  def stretch(): MBRInfo = {

    this.left = math.floor(this.left)
    this.bottom = math.floor(this.bottom)

    this.right = math.ceil(this.right)
    this.top = math.ceil(this.top)

    this
  }

  override def toString: String =
    "%,.4f\t%,.4f\t%,.4f\t%,.4f".format(left, bottom, right, top)

  override def write(kryo: Kryo, output: Output): Unit = {

    output.writeDouble(left)
    output.writeDouble(bottom)
    output.writeDouble(right)
    output.writeDouble(top)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    left = input.readDouble()
    bottom = input.readDouble()
    right = input.readDouble()
    top = input.readDouble()
  }
}

final class RowData extends KryoSerializable {

  var point: Point = _
  var sortedList: SortedLinkedList[Point] = _
  var lstPartitionId: ListBuffer[Int] = _

  def this(point: Point, sortedList: SortedLinkedList[Point], lstPartitionId: ListBuffer[Int]) = {

    this()

    this.point = point
    this.sortedList = sortedList
    this.lstPartitionId = lstPartitionId
  }

  def nextPartId: Int = {

    if (lstPartitionId.nonEmpty) {

      val pId = lstPartitionId.head

      lstPartitionId = lstPartitionId.tail

      pId
    }
    else -1
  }

  override def write(kryo: Kryo, output: Output): Unit = {

    kryo.writeObject(output, point)
    kryo.writeObject(output, sortedList)

    output.writeInt(lstPartitionId.length)
    lstPartitionId.foreach(output.writeInt)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    point = kryo.readObject(input, classOf[Point])
    sortedList = kryo.readObject(input, classOf[SortedLinkedList[Point]])

    val arrLength = input.readInt()

    lstPartitionId = new ListBuffer[Int]()

    (0 until arrLength).foreach(_ => lstPartitionId += input.readInt)
  }
}