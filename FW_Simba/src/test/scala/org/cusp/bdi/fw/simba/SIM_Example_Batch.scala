package org.cusp.bdi.fw.simba

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.Row
import org.apache.spark.sql.simba.SimbaSession
import org.cusp.bdi.util.{CLArgsParser, Helper, LocalRunConsts}

import scala.collection.mutable.{ListBuffer, SortedSet}
import scala.util.Random

object SIM_Example_Batch extends Serializable {

  private val inputPath = "/media/ayman/Data/GeoMatch_Files/InputFiles/RandomSamples/"
  private val inputPathOld = "/media/ayman/Data/GeoMatch_Files/InputFiles/RandomSamples_OLD/"
  private val outputPath = "/media/ayman/Data/GeoMatch_Files/OutputFiles/"

  private val arrInputFiles = Array(
    (inputPath + "Taxi_1_A.csv", inputPath + "Taxi_1_B.csv"),
    (inputPath + "Taxi_2_A.csv", inputPath + "Taxi_2_B.csv"),
    (inputPath + "Taxi_3_A.csv", inputPath + "Taxi_3_B.csv"),
    (inputPath + "Bus_1_A.csv", inputPath + "Bus_1_B.csv"),
    (inputPath + "Bus_2_A.csv", inputPath + "Bus_2_B.csv"),
    (inputPath + "Bus_3_A.csv", inputPath + "Bus_3_B.csv"),
    (inputPath + "Bread_1_A.csv", inputPath + "Bread_1_B.csv"),
    (inputPath + "Bread_2_A.csv", inputPath + "Bread_2_B.csv"),
    (inputPath + "Bread_3_A.csv", inputPath + "Bread_3_B.csv"),
    (inputPathOld + "Taxi_1_A.csv", inputPathOld + "Taxi_1_B.csv"),
    (inputPathOld + "Taxi_2_A.csv", inputPathOld + "Taxi_2_B.csv"),
    (inputPathOld + "Taxi_3_A.csv", inputPathOld + "Taxi_3_B.csv"),
    (inputPathOld + "Bus_1_A.csv", inputPathOld + "Bus_1_B.csv"),
    (inputPathOld + "Bus_2_A.csv", inputPathOld + "Bus_2_B.csv"),
    (inputPathOld + "Bus_3_A.csv", inputPathOld + "Bus_3_B.csv"),
    (inputPathOld + "Bread_1_A.csv", inputPathOld + "Bread_1_B.csv"),
    (inputPathOld + "Bread_2_A.csv", inputPathOld + "Bread_2_B.csv"),
    (inputPathOld + "Bread_3_A.csv", inputPathOld + "Bread_3_B.csv"))

  def main(args: Array[String]): Unit = {

    val lstOutputs = ListBuffer[(Double, String)]()

    arrInputFiles.foreach(inputFile => {

      val startTime = System.currentTimeMillis()

      val args = "-outDir " + outputPath + Random.nextInt(999) + "_Simba_" + inputFile._1.substring(inputFile._1.lastIndexOf("/") + 1, inputFile._1.lastIndexOf("_")) +
        " -firstSet " + inputFile._1 +
        " -firstSetObj " + LocalRunConsts.DS_CODE_THREE_PART_LINE +
        " -secondSet " + inputFile._2 +
        " -secondSetObj " + LocalRunConsts.DS_CODE_THREE_PART_LINE +
        " -matchCount 10" +
        " -queryType kNNJoin"

      val clArgs = CLArgsParser(args.split(' '), SIM_Arguments())

      println("CL Args: " + clArgs.toString)

      var simbaBuilder = SimbaSession.builder()
        .appName(StringBuilder.newBuilder
          .append("SIM_Example_")
          .append("_")
          .append(clArgs.getParamValueString(SIM_Arguments.queryType))
          .append("_")
          .append(clArgs.getParamValueString(SIM_Arguments.firstSetObj))
          .append("_")
          .append(clArgs.getParamValueString(SIM_Arguments.secondSetObj))
          .toString)
        .config("simba.join.partitions", "50")
        .config("simba.index.partitions", "50")
        .config("spark.local.dir", LocalRunConsts.sparkWorkDir)

      simbaBuilder.master("local[*]")

      val simbaSession = simbaBuilder.getOrCreate()

      // delete output dir if exists
      val hdfs = FileSystem.get(simbaSession.sparkContext.hadoopConfiguration)
      val path = new Path(clArgs.getParamValueString(SIM_Arguments.outDir))
      if (hdfs.exists(path))
        hdfs.delete(path, true)

      val DS1 = getDS(simbaSession, clArgs.getParamValueString(SIM_Arguments.firstSet), clArgs.getParamValueString(SIM_Arguments.firstSetObj))
        .repartition(1024)

      val DS2 = getDS(simbaSession, clArgs.getParamValueString(SIM_Arguments.secondSet), clArgs.getParamValueString(SIM_Arguments.secondSetObj))
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

      res1.union(res2).saveAsTextFile(clArgs.getParamValueString(SIM_Arguments.outDir), classOf[GzipCodec])

      simbaSession.stop()

      lstOutputs.append(((System.currentTimeMillis() - startTime) / 1000.0, clArgs.getParamValueString(SIM_Arguments.outDir)))

      printf(">>Total Time: %,.2f Sec%n", lstOutputs.last._1)
      println(">>Output idr: " + lstOutputs.last._2)
    })

    lstOutputs.foreach(println)
  }

  private final def getDS(simbaSession: SimbaSession, fileName: String, objType: String) = {

    //        import simbaSession.implicits._
    //        import simbaSession.simbaImplicits._

    import simbaSession.implicits._

    simbaSession.read.textFile(fileName)
      .map(SimbaLineParser.lineParser(objType))
      .filter(_ != null)
      .map(row => PointData(row._2._1.toDouble, row._2._2.toDouble, row._1))
  }

  def rowToString(row: (String, SortedSet[(Double, String)])) = {
    val sb = StringBuilder.newBuilder
      .append(row._1)

    (SortedSet[(Double, String)]() ++ row._2)
      .foreach(matches => sb.append(";%.8f,%s".format(matches._1, matches._2)))

    sb.toString()
  }

  def processRow(row: Row) = {

    val x = row(0).toString.toDouble
    val y = row(1).toString.toDouble

    val dist = Helper.euclideanDist((x, y), (row(3) match {
      case d: Double => d
    }, row(4) match {
      case d: Double => d
    }))

    val sSet = SortedSet((dist, row(5).toString))

    val pointInfo = "%s,%.8f,%.8f".format(row.get(2).toString, x, y)

    (pointInfo, sSet)
  }

  case class PointData(x: Double, y: Double, other: String)

}