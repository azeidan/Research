package cs.purdue.edu.examples

import com.vividsolutions.jts.io.WKTReader
import cs.purdue.edu.spatialindex.rtree.{ Box, Point }
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.Util
import org.apache.spark.{ SparkConf, SparkContext }

import scala.util.Try
import org.apache.log4j.Logger

/**
  * Created by merlin on 6/8/16.
  */
object SpatialJoinApp_TLC {

    val LOGGER = Logger.getLogger(SpatialJoinApp_TLC.getClass.getName);

    val usage = """
    Implementation of Spatial Join on Spark
    Usage: spatialjoin --left left_data
                       --right right_data
                       --index the local index for spatial data (default:rtree)
                       --help
              """

    def main(args: Array[String]) {

        if (args.length == 0) println(usage)

        val arglist = args.toList
        type OptionMap = Map[Symbol, Any]

        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
                case Nil => map
                case "--help" :: tail =>
                    println(usage)
                    sys.exit(0)
                case "--left" :: value :: tail =>
                    nextOption(map ++ Map('left -> value), tail)
                case "--right" :: value :: tail =>
                    nextOption(map ++ Map('right -> value), tail)
                case "--index" :: value :: tail =>
                    nextOption(map = map ++ Map('index -> value), list = tail)
                case "--out" :: value :: tail =>
                    nextOption(map = map ++ Map('out -> value), list = tail)
                case option :: tail =>
                    println("Unknown option " + option)
                    sys.exit(1)
            }
        }

        val options = nextOption(Map(), arglist)

        val leftFile = options.getOrElse('left, Nil).asInstanceOf[String]
        val rightFile = options.getOrElse('right, Nil).asInstanceOf[String]
        val outputDir = options.getOrElse('out, Nil).asInstanceOf[String]
        Util.localIndex = options.getOrElse('index, Nil).asInstanceOf[String]

        val conf = new SparkConf().setAppName("Test for Spatial JOIN SpatialRDD")

        val spark = new SparkContext(conf)

        val leftpoints = spark.textFile(leftFile)
            .filter(tlcRow => {

                // Filter out invalid coordinates and out-of-range dates if a date range is set

                val arr = tlcRow.split(",")
                var result = false

                try {

                    var lon = arr(5).toDouble
                    var lat = arr(6).toDouble

                    result = lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90
                }
                catch {
                    case ex: Exception => {
                        LOGGER.info("Skipping row: " + tlcRow + ex.toString())
                    }
                }

                result
            })
            .map(row => {

                val arr = row.split(",")

                val pickupLon = String.format("%.6f", arr(5).toFloat.asInstanceOf[Object]).toFloat
                val pickupLat = String.format("%.6f", arr(6).toFloat.asInstanceOf[Object]).toFloat

                (Point(pickupLon.toFloat, pickupLat.toFloat), "1")
            })
        val leftLocationRDD = SpatialRDD(leftpoints).cache()

        /** **********************************************************************************/

        /** **********************************************************************************/
        val rightData = spark.textFile(rightFile)
        val rightBoxes = rightData.map(x =>
            {
                val arr = x.split(",")

                Box(arr(0).toFloat, arr(1).toFloat, arr(4).toFloat, arr(5).toFloat)
            })

        def aggfunction1[K, V](itr: Iterator[(K, V)]): Int = {
            itr.size
        }

        def aggfunction2(v1: Int, v2: Int): Int = {
            v1 + v2
        }

        /** **********************************************************************************/
        var b1 = System.currentTimeMillis

        val joinresultRdd = leftLocationRDD.rjoin(rightBoxes)(aggfunction1, aggfunction2)

        println("the outer table size: " + rightBoxes.count())
        println("the inner table size: " + leftpoints.count())
        val tuples = joinresultRdd.map { case (b, v) => (1, v) }.reduceByKey { case (a, b) => { a + b } }.map { case (a, b) => b }.collect()

        spark.parallelize(tuples).saveAsTextFile(outputDir)

        println("global index: " + Util.localIndex + " ; local index: " + Util.localIndex)
        //        println("query results size: " + tuples(0))
        println("spatial range join time: " + (System.currentTimeMillis - b1) + " (ms)")

        spark.stop()

    }
}