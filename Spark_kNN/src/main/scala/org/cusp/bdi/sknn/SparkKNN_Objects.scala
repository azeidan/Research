package org.cusp.bdi.sknn

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import org.cusp.bdi.ds._
import org.cusp.bdi.ds.geom.Point
import org.cusp.bdi.ds.sortset.SortedList
import org.cusp.bdi.sknn.util._

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

  def this(start: ((Double, Double), Long)) = {

    this()

    this.lstMBRCoord += start
    this.totalWeight = start._2
    this.left = start._1._1
    this.bottom = start._1._2
    this.right = start._1._1
    this.top = start._1._2
  }

  def mbr: (Double, Double, Double, Double) =
    (left, bottom, right, top)

  override def toString: String =
    "%f\t%f\t%f\t%f\t%d".format(left, bottom, right, top, totalWeight)
}

final class RowData extends KryoSerializable {

  var point: Point = _
  var sortedList: SortedList[Point] = _
  var lstPartitionId: ListBuffer[Int] = _

  def this(point: Point, sortedList: SortedList[Point], lstPartitionId: ListBuffer[Int]) = {

    this()

    this.point = point
    this.sortedList = sortedList
    this.lstPartitionId = lstPartitionId
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

    sortedList = kryo.readClassAndObject(input).asInstanceOf[SortedList[Point]]
    lstPartitionId = kryo.readClassAndObject(input).asInstanceOf[ListBuffer[Int]]
  }
}

case class BroadcastWrapper(spatialIdx: SpatialIndex, gridOp: GridOperation, arrPartitionMBRs: Array[(Double, Double, Double, Double)]) extends Serializable {}
