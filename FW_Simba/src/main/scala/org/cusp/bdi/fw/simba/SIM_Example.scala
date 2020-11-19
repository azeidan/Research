package org.cusp.bdi.fw.simba

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.simba.{DataFrame, SimbaSession}
import org.cusp.bdi.util.{Arguments, CLArgsParser, InputFileParsers, LocalRunConsts}

import scala.collection.mutable

case class PointData(x: Double, y: Double, other: String)

object SIM_Example extends Serializable {

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()

    //        val clArgs = SIM_CLArgs.taxi_taxi_1M_No_Trip
    //        val clArgs = SIM_CLArgs.randomPoints_randomPoints
    //        val clArgs = SIM_CLArgs.busPoint_busPointShift
    //        val clArgs = SIM_CLArgs.TPEP_Point_TPEP_Point
    //        val clArgs = SIM_CLArgs.lion_PolyRect_TPEP_Point
    //        val clArgs = SIM_CLArgs.lion_LStr_TPEP_Point
    //        val clArgs = SIM_CLArgs.lion_LStr_Taxi_Point
    //        val clArgs = SIM_CLArgs.lion_PolyRect_Taxi_Point
    //        val clArgs = SIM_CLArgs.OSM_Point_OSM_Point

    val clArgs = Simba_Local_CLArgs.random_sample
    //        val clArgs = CLArgsParser(args, Arguments_Simba.lstArgInfo())

    val simbaBuilder = SimbaSession.builder()
      .appName(this.getClass.getName)
      //      .config("simba.index.partitions", "64") // from Simba's examples
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

    val DS2 = getDS(simbaSession, clArgs.getParamValueString(Arguments.secondSet), clArgs.getParamValueString(Arguments.secondSetObjType))

    import simbaSession.implicits._
    import simbaSession.simbaImplicits._

    if (clArgs.getParamValueBoolean(Arguments_Simba.sortByEuclDist))
      DS1.knnJoin(DS2, Array("x", "y"), Array("x", "y"), 10)
        .rdd
        .mapPartitions(_.map(processRow))
        .reduceByKey(_ ++ _)
        //        .union(
        //          DS2.knnJoin(DS1, Array("x", "y"), Array("x", "y"), 10)
        //            .rdd
        //            .mapPartitions(_.map(processRow))
        //            .reduceByKey(_ ++ _)
        //        )
        .mapPartitions(_.map(rowToString))
        .saveAsTextFile(clArgs.getParamValueString(Arguments.outDir), classOf[GzipCodec])
    else
      DS1.knnJoin(DS2, Array("x", "y"), Array("x", "y"), 10).rdd
        //        .union(DS2.knnJoin(DS1, Array("x", "y"), Array("x", "y"), 10).rdd)
        .mapPartitions(_.map(row => "%s,%.8f,%.8f".format(row.get(2).toString, row(0).toString.toDouble, row(1).toString.toDouble)))
        .saveAsTextFile(clArgs.getParamValueString(Arguments.outDir), classOf[GzipCodec])

    simbaSession.stop()

    if (clArgs.getParamValueBoolean(Arguments.local)) {

      LocalRunConsts.logLocalRunEntry(LocalRunConsts.localRunLogFile, "Simba",
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
}