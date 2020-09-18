package org.cusp.bdi.fw.simba

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.Row
import org.apache.spark.sql.simba.SimbaSession
import org.cusp.bdi.util.{Arguments, CLArgsParser, InputFileParsers, LocalRunConsts}

import scala.collection.mutable

object SIM_Example extends Serializable {

  //  private val inputPath = "/media/ayman/Data/GeoMatch_Files/InputFiles/RandomSamples/"
  //  private val inputPathOld = "/media/ayman/Data/GeoMatch_Files/InputFiles/RandomSamples_OLD/"

  //    case class PointData(x: Double, y: Double)
  //    case class LineSegmentData(start: Point, end: Point, other: String)

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    //    val clArgs = SIM_CLArgs.random_sample

    val clArgs = CLArgsParser(args, Arguments.lstArgInfo())
    //        val clArgs = SIM_CLArgs.taxi_taxi_1M_No_Trip
    //        val clArgs = SIM_CLArgs.randomPoints_randomPoints
    //        val clArgs = SIM_CLArgs.busPoint_busPointShift
    //        val clArgs = SIM_CLArgs.TPEP_Point_TPEP_Point
    //        val clArgs = SIM_CLArgs.lion_PolyRect_TPEP_Point
    //        val clArgs = SIM_CLArgs.lion_LStr_TPEP_Point
    //        val clArgs = SIM_CLArgs.lion_LStr_Taxi_Point
    //        val clArgs = SIM_CLArgs.lion_PolyRect_Taxi_Point
    //        val clArgs = SIM_CLArgs.OSM_Point_OSM_Point

    val simbaBuilder = SimbaSession.builder()
      .appName(this.getClass.getName)
      .config("simba.join.partitions", "50")
      .config("simba.index.partitions", "50")
      .config("spark.local.dir", LocalRunConsts.sparkWorkDir)

    if (clArgs.getParamValueBoolean(Arguments.local)) {
      simbaBuilder.config("spark.local.dir", LocalRunConsts.sparkWorkDir)
      simbaBuilder.master("local[*]")
    }

    val simbaSession = simbaBuilder.getOrCreate()

    // delete output dir if exists
    val hdfs = FileSystem.get(simbaSession.sparkContext.hadoopConfiguration)
    val path = new Path(clArgs.getParamValueString(Arguments.outDir))
    if (hdfs.exists(path))
      hdfs.delete(path, true)

    val DS1 = getDS(simbaSession, clArgs.getParamValueString(Arguments.firstSet), clArgs.getParamValueString(Arguments.firstSetObjType))
      .repartition(1024)

    val DS2 = getDS(simbaSession, clArgs.getParamValueString(Arguments.secondSet), clArgs.getParamValueString(Arguments.secondSetObjType))
      .repartition(1024)

    //        import simbaSession.implicits._
    //        import simbaSession.simbaImplicits._

    import simbaSession.implicits._
    import simbaSession.simbaImplicits._

    val res1 = DS1.knnJoin(DS2, Array("x", "y"), Array("x", "y"), 10)
      .rdd
      .mapPartitions(_.map(processRow))
      .reduceByKey(_ ++ _)
      .mapPartitions(_.map(rowToString))

    val res2 = DS2.knnJoin(DS1, Array("x", "y"), Array("x", "y"), 10)
      .rdd
      .mapPartitions(_.map(processRow))
      .reduceByKey(_ ++ _)
      .mapPartitions(_.map(rowToString))

    res1.union(res2).saveAsTextFile(clArgs.getParamValueString(Arguments.outDir), classOf[GzipCodec])

    simbaSession.stop()

    if (clArgs.getParamValueBoolean(Arguments.local)) {

      LocalRunConsts.logLocalRunEntry(LocalRunConsts.localRunLogFile, "sKNN",
        clArgs.getParamValueString(Arguments.firstSet).substring(clArgs.getParamValueString(Arguments.firstSet).lastIndexOf("/") + 1),
        clArgs.getParamValueString(Arguments.secondSet).substring(clArgs.getParamValueString(Arguments.secondSet).lastIndexOf("/") + 1),
        clArgs.getParamValueString(Arguments.outDir).substring(clArgs.getParamValueString(Arguments.outDir).lastIndexOf("/") + 1),
        (System.currentTimeMillis() - startTime) / 1000.0)

      printf("Total Time: %,.4f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
      println("Output: %s".format(clArgs.getParamValueString(Arguments.outDir)))
      println("Run Log: %s".format(LocalRunConsts.localRunLogFile))
    }
  }

  private final def getDS(simbaSession: SimbaSession, fileName: String, objType: String) = {

    //        import simbaSession.implicits._
    //        import simbaSession.simbaImplicits._

    import simbaSession.implicits._

    simbaSession.read.textFile(fileName)
      .map(InputFileParsers.getLineParser(objType))
      .filter(_ != null)
      .map(row => PointData(row._2._1.toDouble, row._2._2.toDouble, row._1))
  }

  def rowToString(row: (String, mutable.SortedSet[(Double, String)])): String = {
    val sb = StringBuilder.newBuilder
      .append(row._1)

    (mutable.SortedSet[(Double, String)]() ++ row._2)
      .foreach(matches => sb.append(";%.8f,%s".format(matches._1, matches._2)))

    sb.toString()
  }

  private def euclideanDist(xy1: (Double, Double), xy2: (Double, Double)) =
    math.sqrt(math.pow(xy1._1 - xy2._1, 2) + math.pow(xy1._2 - xy2._2, 2))

  def processRow(row: Row): (String, mutable.SortedSet[(Double, String)]) = {


    val x = row(0).toString.toDouble
    val y = row(1).toString.toDouble

    val dist = euclideanDist((x, y), (row(3) match {
      case d: Double => d
    }, row(4) match {
      case d: Double => d
    }))

    val sSet = mutable.SortedSet((dist, row(5).toString))

    val pointInfo = "%s,%.8f,%.8f".format(row.get(2).toString, x, y)

    (pointInfo, sSet)
  }

  case class PointData(x: Double, y: Double, other: String)

}