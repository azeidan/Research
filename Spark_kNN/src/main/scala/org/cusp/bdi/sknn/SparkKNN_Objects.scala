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

final class RangeInfo {

  val lstMBRCoord: ListBuffer[((Double, Double), Long)] = ListBuffer[((Double, Double), Long)]()
  var totalWeight = 0L
  var left: Double = _
  var bottom: Double = _
  var right: Double = _
  var top: Double = _

  def this(seed: ((Double, Double), Long)) = {

    this()

    this.lstMBRCoord += seed
    this.totalWeight = seed._2
    this.left = seed._1._1
    this.bottom = seed._1._2
    this.right = seed._1._1
    this.top = seed._1._2
  }

  def mbr: (Double, Double, Double, Double) =
    (left, bottom, right, top)

  override def toString: String =
    "%f\t%f\t%f\t%f\t%,d".format(left, bottom, right, top, totalWeight)
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

    var pId = -1

    if (lstPartitionId.nonEmpty) {

      pId = lstPartitionId.head

      lstPartitionId = lstPartitionId.tail
    }

    pId
  }

  override def write(kryo: Kryo, output: Output): Unit = {

    kryo.writeClassAndObject(output, point)
    kryo.writeClassAndObject(output, sortedList)
    kryo.writeClassAndObject(output, lstPartitionId)
  }

  override def read(kryo: Kryo, input: Input): Unit = {

    point = kryo.readClassAndObject(input) match {
      case pt: Point => pt
    }

    sortedList = kryo.readClassAndObject(input).asInstanceOf[SortedLinkedList[Point]]
    lstPartitionId = kryo.readClassAndObject(input).asInstanceOf[ListBuffer[Int]]
  }
}

//case class BroadcastWrapper(spatialIdx: SpatialIndex, arrPartitionMBRs: Array[(Double, Double, Double, Double)]) extends Serializable {}
