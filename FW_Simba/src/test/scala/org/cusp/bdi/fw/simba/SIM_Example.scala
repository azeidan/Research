package org.cusp.bdi.fw.simba

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.Row
import org.apache.spark.sql.simba.SimbaSession
import org.cusp.bdi.fw.simba.Simba_Local_CLArgs.SupportedKnnOperations
import org.cusp.bdi.util.{Arguments, Helper, InputFileParsers, LocalRunConsts}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

    val clArgs = Simba_Local_CLArgs.random_sample()
    //        val clArgs = CLArgsParser(args, Arguments_Simba.lstArgInfo())

    val simbaBuilder = SimbaSession.builder()
      .appName(this.getClass.getName)
      //      .config("simba.index.partitions", "64") // from Simba's examples
      .config("spark.local.dir", LocalRunConsts.sparkWorkDir)

    if (clArgs.getParamValueBoolean(Arguments.local)) {

      simbaBuilder.master("local[*]")
      simbaBuilder.config("spark.local.dir", LocalRunConsts.sparkWorkDir)
      simbaBuilder.config("spark.driver.memory", clArgs.getParamValueString(Arguments.driverMemory))
      simbaBuilder.config("spark.executor.memory", clArgs.getParamValueString(Arguments.executorMemory))
      simbaBuilder.config("spark.executor.instances", clArgs.getParamValueString(Arguments.numExecutors))
      simbaBuilder.config("spark.executor.cores", clArgs.getParamValueString(Arguments.executorCores))
    }

    val simbaSession = simbaBuilder.getOrCreate()

    val hdfs = FileSystem.get(simbaSession.sparkContext.hadoopConfiguration)
    val outDir = new Path(clArgs.getParamValueString(Arguments.outDir))
    val kParam = clArgs.getParamValueInt(Arguments.k)

    // delete output dir if exists
    if (hdfs.exists(outDir))
      hdfs.delete(outDir, true)

    import simbaSession.implicits._
    import simbaSession.simbaImplicits._

    val DS1 = simbaSession.read.textFile(clArgs.getParamValueString(Arguments.firstSet))
      .map(InputFileParsers.getLineParser(clArgs.getParamValueString(Arguments.firstSetObjType)))
      .filter(_ != null)
      .map(row => PointData(row._2._1.toDouble, row._2._2.toDouble, row._1))
    //      .limit(100)
    val DS2 = simbaSession.read.textFile(clArgs.getParamValueString(Arguments.secondSet))
      .map(InputFileParsers.getLineParser(clArgs.getParamValueString(Arguments.secondSetObjType)))
      .filter(_ != null)
      .map(row => PointData(row._2._1.toDouble, row._2._2.toDouble, row._1))
    //      .limit(100)

    (clArgs.getParamValueString(Arguments.knnJoinType) match {
      case s if s eq SupportedKnnOperations.allKnn.toString =>
        DS1.knnJoin(DS2, Array("x", "y"), Array("x", "y"), kParam)
          .union(DS2.knnJoin(DS1, Array("x", "y"), Array("x", "y"), kParam))
      case s if s eq SupportedKnnOperations.knn.toString =>
        DS1.knnJoin(DS2, Array("x", "y"), Array("x", "y"), kParam)
    })
      .mapPartitions(_.map(processRow))
      .rdd
      .reduceByKey(_ ++ _)
      .mapPartitions(_.map(row => "%s;%s".format(row._1, row._2.map(row => "%.8f,%s".format(row._1, row._2)).mkString(";"))))
      .saveAsTextFile(clArgs.getParamValueString(Arguments.outDir), classOf[GzipCodec])

    /*
        DS1.knnJoin(DS2, Array("x", "y"), Array("x", "y"), kParam)
          .mapPartitions(_.map(processRow))
          .rdd
          .reduceByKey(_ ++ _)
          //        .union(
          //          DS2.knnJoin(DS1, Array("x", "y"), Array("x", "y"), kParam)
          //            .rdd
          //            .mapPartitions(_.map(processRow))
          //            .reduceByKey(_ ++ _)
          //        )
          .mapPartitions(_.map(row =>
            "%s;%s".format(row._1, row._2.map(distData => ("%.8f,%s".format(distData._1, distData._2))).mkString(";"))))
          .saveAsTextFile(clArgs.getParamValueString(Arguments.outDir), classOf[GzipCodec])
        else
        DS1.knnJoin(DS2, Array("x", "y"), Array("x", "y"), kParam).rdd
          //        .union(DS2.knnJoin(DS1, Array("x", "y"), Array("x", "y"), kParam).rdd)
          .mapPartitions(_.map(row => "%s,%.8f,%.8f".format(row.get(2).toString, row(0).toString.toFloat, row(1).toString.toFloat)))
          .saveAsTextFile(clArgs.getParamValueString(Arguments.outDir), classOf[GzipCodec])
    */

    simbaSession.stop()

    if (clArgs.getParamValueBoolean(Arguments.local)) {

      val message = "%s\t%s\t%s\t%s\t%.4f%n".format("Simba",
        clArgs.getParamValueString(Arguments.firstSet).substring(clArgs.getParamValueString(Arguments.firstSet).lastIndexOf("/") + 1),
        clArgs.getParamValueString(Arguments.secondSet).substring(clArgs.getParamValueString(Arguments.secondSet).lastIndexOf("/") + 1),
        clArgs.getParamValueString(Arguments.outDir).substring(clArgs.getParamValueString(Arguments.outDir).lastIndexOf("/") + 1),
        (System.currentTimeMillis() - startTime) / 1000.0
      )

      LocalRunConsts.logLocalRunEntry(LocalRunConsts.localRunLogFile, message, null, null)

      printf("Total Time: %,.4f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
      println("Output: %s".format(clArgs.getParamValueString(Arguments.outDir)))
      println("Run Log: %s".format(LocalRunConsts.localRunLogFile))
    }
  }

  //
  //  private final def getDS(simbaSession: SimbaSession, fileName: String, objType: String) = {
  //
  //    //        import simbaSession.implicits._
  //    //        import simbaSession.simbaImplicits._
  //
  //    import simbaSession.implicits._
  //
  //    simbaSession.read.textFile(fileName)
  //      .map(InputFileParsers.getLineParser(objType))
  //      .filter(_ != null)
  //      .map(row => PointData(row._2._1.toDouble, row._2._2.toDouble, row._1))
  //  }

  def rowToString(row: (String, mutable.SortedSet[(Double, String)])): String = {

    val sb = StringBuilder.newBuilder
      .append(row._1)

    (mutable.SortedSet[(Double, String)]() ++ row._2)
      .foreach(matches => sb.append(";%.8f,%s".format(matches._1, matches._2)))

    sb.toString()
  }

  private def euclideanDist(x1: Double, y1: Double, x2: Double, y2: Double): Double =
    math.sqrt(Helper.squaredEuclideanDist(x1, y1, x2, y2))

  def processRow(row: Row) = {

    val x1 = row(0).toString.toDouble
    val y1 = row(1).toString.toDouble

    val x2 = row(3) match {
      case d: Double => d
    }

    val y2 = row(4) match {
      case d: Double => d
    }

    //    val sSet = mutable.SortedSet((dist, row(5).toString))

    val pointInfo = "%s,%.8f,%.8f".format(row.get(2).toString, x1, y1)

    (pointInfo, ListBuffer((euclideanDist(x1, y1, x2, y2), row(5).toString)))
  }
}