package org.cusp.bdi.fw.simba

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.Row
import org.apache.spark.sql.simba.SimbaSession
import org.cusp.bdi.util.{Helper, LocalRunConsts}

import scala.collection.mutable.SortedSet

object SIM_Example extends Serializable {

    case class PointData(x: Double, y: Double, other: String)
    //    case class PointData(x: Double, y: Double)
    //    case class LineSegmentData(start: Point, end: Point, other: String)

    private final def getDS(simbaSession: SimbaSession, fileName: String, objType: String) = {

        //        import simbaSession.implicits._
        //        import simbaSession.simbaImplicits._

        import simbaSession.implicits._

        simbaSession.read.textFile(fileName)
            .map(SimbaLineParser.lineParser(objType))
            .filter(_ != null)
            .map(row => PointData(row._2._1.toDouble, row._2._2.toDouble, row._1))
    }

    def main(args: Array[String]): Unit = {

        val startTime = System.currentTimeMillis()

        //        val clArgs = SIM_CLArgs.taxi_taxi_1M_No_Trip
        val clArgs = SIM_CLArgs.random_sample
        //        val clArgs = SIM_CLArgs.randomPoints_randomPoints

        //        val clArgs = SIM_CLArgs.busPoint_busPointShift
        //        val clArgs = SIM_CLArgs.TPEP_Point_TPEP_Point
        //        val clArgs = SIM_CLArgs.lion_PolyRect_TPEP_Point
        //        val clArgs = SIM_CLArgs.lion_LStr_TPEP_Point
        //        val clArgs = SIM_CLArgs.lion_LStr_Taxi_Point
        //        val clArgs = SIM_CLArgs.lion_PolyRect_Taxi_Point
        //        val clArgs = SIM_CLArgs.OSM_Point_OSM_Point
        //        val clArgs = CLArgsParser(args, SIM_Arguments())

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
        //            .getOrCreate()

        if (clArgs.getParamValueBoolean(SIM_Arguments.local))
            simbaBuilder.master("local[*]")

        val simbaSession = simbaBuilder.getOrCreate()

        // delete output dir if exists
        Helper.delDirHDFS(simbaSession.sparkContext, clArgs.getParamValueString(SIM_Arguments.outDir))

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

        if (clArgs.getParamValueBoolean(SIM_Arguments.local)) {

            printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
            println("Output idr: " + clArgs.getParamValueString(SIM_Arguments.outDir))
        }
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

        val dist = Helper.euclideanDist((x, y), (row(3) match { case d: Double => d }, row(4) match { case d: Double => d }))

        val sSet = SortedSet((dist, row(5).toString))

        val pointInfo = "%s,%.8f,%.8f".format(row.get(2).toString, x, y)

        (pointInfo, sSet)
    }
}