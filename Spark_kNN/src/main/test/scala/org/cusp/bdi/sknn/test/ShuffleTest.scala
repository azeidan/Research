package org.cusp.bdi.sknn.test

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.network.shuffle.OneForOneBlockFetcher
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.cusp.bdi.util.{Helper, InputFileParsers}

import scala.util.Random

//abstract class Key() extends KryoSerializable with Ordered[Key] {
//
//  var pIdx: Int = 0
//
//  def this(pIdx: Int) = {
//
//    this()
//    this.pIdx = pIdx
//  }
//
//  /*
//   *  - negative if x < y
//   *  - positive if x > y
//   *  - zero otherwise (if x == y)
//   */
//  override def compare(that: Key): Int =
//    this match {
//      case _: KeySI => -1
//      case _ =>
//        that match {
//          case _: KeySI => 1
//          case _ => -1
//        }
//    }
//
//  override def toString: String =
//    this.getClass.getName + " " + pIdx
//
//  override def write(kryo: Kryo, output: Output): Unit =
//    output.writeInt(pIdx)
//
//  override def read(kryo: Kryo, input: Input): Unit =
//    pIdx = input.readInt()
//}
//
//final class KeySI(pIdx: Int) extends Key(pIdx) {}
//
//final class KeyPoint(pIdx: Int) extends Key(pIdx) {}

object ShuffleTest {

  def main(args: Array[String]): Unit = {

    //    val set = mutable.SortedSet[Key]()
    //    set += new KeySI(0)
    //    set += new KeyPoint(1)
    //    set += new KeySI(2)
    //    set += new KeyPoint(4)
    //    set += new KeyPoint(3)
    //
    //    set.foreach(println)
    val sparkConf = new SparkConf()

    sparkConf.setAppName(this.getClass.getName)
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .set("spark.executor.heartbeatInterval", "40000")
      .set("spark.network.timeout", "150000")

    val sc = new SparkContext(sparkConf)

    val numParts = 23000 /*22843*/

    val partitioner = new Partitioner() {

      override def numPartitions: Int = numParts

      override def getPartition(key: Any): Int =
        Random.nextInt(numParts)
    }

    val outDir = "/gws/projects/project-taxi_capstone_2016/share/sparkKNN_WorkFolder/TBD"

    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(outDir)
    if (hdfs.exists(path)) hdfs.delete(path, true)

    val rddLeft = sc.textFile("/gws/projects/project-taxi_capstone_2016/data/breadcrumb_nad83/part-*")
      .mapPartitions(_.map(InputFileParsers.getLineParser("TPEP_Point")))
      .filter(_ != null)
      .mapPartitionsWithIndex((pIdx, iter) => iter.map(row => (Random.nextInt(numParts), row._1.asInstanceOf[Any])))
      .repartition(numParts)
      .persist()

    var rddRight = sc.textFile("/gws/projects/project-taxi_capstone_2016/data/breadcrumb_nad83/part-*")
      .mapPartitions(_.map(InputFileParsers.getLineParser("TPEP_Point")))
      .filter(_ != null)
      .mapPartitionsWithIndex((pIdx, iter) => iter.map(row => (Random.nextInt(numParts), row._1.asInstanceOf[Any])))

    for (roundNum <- 0 until 26) {

      //      if (roundNum > 0)
      //        rddRes = rddRes.repartitionAndSortWithinPartitions(rddLeft.partitioner.get)

      rddRight = (rddLeft ++ rddRight.repartition(numParts))
        .coalesce(numParts)
        .mapPartitionsWithIndex((pIdx, iter) => {

          Helper.loggerSLf4J(debugOn = true, ShuffleTest, ">>Union: " + pIdx + " roundNum: " + roundNum, null)

          iter.next() // skip

          iter.map(row => (Random.nextInt(numParts), row._2))
        })
    }

    rddRight.count()
  }
}
