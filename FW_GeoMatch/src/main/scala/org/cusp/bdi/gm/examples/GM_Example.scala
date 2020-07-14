package org.cusp.bdi.gm.examples

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.cusp.bdi.gm.GeoMatch
import org.cusp.bdi.gm.geom.GMGeomBase
import org.cusp.bdi.gm.geom.GMLineString
import org.cusp.bdi.gm.geom.GMPoint
import org.cusp.bdi.gm.geom.GMPolygon
import org.cusp.bdi.gm.geom.GMRectangle
import org.cusp.bdi.util.Arguments_QueryType
import org.cusp.bdi.util.CLArgsParser
import org.cusp.bdi.util.Helper
import org.cusp.bdi.util.InputFileParsers

object QueryType {
    val distance = "distance"
    val kNN = "knn"
    val range = "range"
}

object GM_Arguments extends Arguments_QueryType {

    val hilbertN = ("hilbertN", "Int", 256, "The size of the hilbert curve (i.e. n)")
    val adjustForBoundCross = ("adjustForBoundCross", "Boolean", false, "Set to true to adust search to account for second dataset objects that cross partition boundries. This will increase 1st set obj duplication.")
    val searchGridMinX = ("searchGridMinX", "Int", -1, "Search grid's minimum X")
    val searchGridMinY = ("searchGridMinY", "Int", -1, "Search grid's minimum Y")
    val searchGridMaxX = ("searchGridMaxX", "Int", -1, "Search grid's maximum X")
    val searchGridMaxY = ("searchGridMaxY", "Int", -1, "Search grid's maximum Y")

    override def apply() =
        super.apply() ++ List(queryType, hilbertN, adjustForBoundCross, searchGridMinX, searchGridMinY, searchGridMaxX, searchGridMaxY)
}

object GM_Example extends Serializable {

    private def RDD_Point(sparkContext: SparkContext, objType: String, fileName: String) = {

        val lineParser = objType match {
            case s if s matches "(?i)TPEP_Point" => InputFileParsers.tpepPoints
            case s if s matches "(?i)Taxi_Point" => InputFileParsers.taxiPoints
            case s if s matches "(?i)Bus_Point" => InputFileParsers.busPoints
        }

        sparkContext.textFile(fileName)
            .mapPartitions(_.map(lineParser))
            .filter(_ != null)
            .mapPartitions(_.map(parts => new GMPoint(parts._1, (parts._2._1.toInt + 1, parts._2._2.toInt + 1))))
    }
    private def getRDD(sparkContext: SparkContext, objType: String, fileName: String, errorRange: Int): RDD[_ <: GMGeomBase] = {

        if (objType.matches("(?i)TPEP_Point|(?i)Taxi_Point|(?i)Bus_Point"))
            RDD_Point(sparkContext, objType, fileName)
        else if (objType.matches("(?i)LION_LineString|(?i)LION_Polygon|(?i)LION_Rectangle"))
            sparkContext.textFile(fileName)
                .mapPartitions(_.map(InputFileParsers.nycLION))
                .filter(_ != null)
                .mapPartitions(_.map(parts => {

                    val arrCoords = parts._2.map(coords => {

                        val x = coords._1.substring(0, coords._1.indexOf('.')).toInt + 1 // ceil
                        val y = coords._2.substring(0, coords._2.indexOf('.')).toInt + 1 // ceil

                        (x, y)
                    })

                    (parts._1, arrCoords)
                }))
                .mapPartitions(_.map(row =>
                    objType match {
                        case s if s matches "(?i)LION_LineString" =>
                            new GMLineString(row._1, row._2)
                        case s if s matches "(?i)LION_Polygon" =>
                            new GMPolygon(row._1, row._2)
                        case s if s matches "(?i)LION_Rectangle" => {

                            val mbr = Helper.getMBREnds(row._2, errorRange)

                            new GMRectangle(row._1, mbr(0), mbr(1))
                        }
                    }))
        else if (objType.matches("(?i)TPEP_Point|(?i)Taxi_Point|(?i)Bus_Point"))
            RDD_Point(sparkContext, objType, fileName)
        else
            throw new Exception(objType + " Is not a valid object type")
    }

    def main(args: Array[String]): Unit = {

        val startTime = System.currentTimeMillis()

        // change the match expression based on the values to run. 0 is for running on the cluster
        //        val clArgs = GM_LocalRunArgs.lion_LineStr_TPEP_Point
        //                val clArgs = GM_LocalRunArgs.TPEP_Point_TPEP_Point
        //        val clArgs = GM_LocalRunArgs.lion_Poly_TPEP_Point
        //        val clArgs = GM_LocalRunrgs.lion_PolyRect_TPEP_Point
        //        val clArgs = GM_LocalRunArgs.lion_LineStr_Taxi_Point
        //        val clArgs = GM_LocalRunArgs.lion_Poly_Taxi_Point
        //        val clArgs = GM_LocalRunArgs.lion_LineStr_Bus_Point
        val clArgs = CLArgsParser(args, GM_Arguments())

        val sparkConf = new SparkConf()
            .setAppName(StringBuilder
                .newBuilder
                .append("GM_Example_")
                .append("_")
                .append(clArgs.getParamValueString(GM_Arguments.queryType))
                .append("_")
                .append(clArgs.getParamValueString(GM_Arguments.firstSetObj))
                .append("_")
                .append(clArgs.getParamValueString(GM_Arguments.secondSetObj))
                .toString)

        if (clArgs.getParamValueBoolean(GM_Arguments.local))
            sparkConf.setMaster("local[*]")

        sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
        sparkConf.registerKryoClasses(GeoMatch.getGeoMatchClasses())

        val sparkContext = new SparkContext(sparkConf)

        // delete output dir if exists
        Helper.delDirHDFS(sparkContext, clArgs.getParamValueString(GM_Arguments.outDir))

        val searchGridMBR = (clArgs.getParamValueInt(GM_Arguments.searchGridMinX), clArgs.getParamValueInt(GM_Arguments.searchGridMinY), clArgs.getParamValueInt(GM_Arguments.searchGridMaxX), clArgs.getParamValueInt(GM_Arguments.searchGridMaxY))

        val geoMatch = new GeoMatch(clArgs.getParamValueBoolean(GM_Arguments.debug),
                                    clArgs.getParamValueInt(GM_Arguments.hilbertN),
                                    clArgs.getParamValueDouble(GM_Arguments.errorRange),
                                    searchGridMBR)

        val rdd1 = getRDD(sparkContext, clArgs.getParamValueString(GM_Arguments.firstSetObj), clArgs.getParamValueString(GM_Arguments.firstSet), clArgs.getParamValueDouble(GM_Arguments.errorRange).toInt)
        val rdd2 = getRDD(sparkContext, clArgs.getParamValueString(GM_Arguments.secondSetObj), clArgs.getParamValueString(GM_Arguments.secondSet), clArgs.getParamValueDouble(GM_Arguments.errorRange).toInt)
        val matchCount = clArgs.getParamValueInt(GM_Arguments.matchCount)
        val matchDist = clArgs.getParamValueDouble(GM_Arguments.matchDist)
        val adjustForBoundCross = clArgs.getParamValueBoolean(GM_Arguments.adjustForBoundCross)

        val rddResult = clArgs.getParamValueString(GM_Arguments.queryType).toLowerCase() match {
            case QueryType.distance =>
                geoMatch.spatialJoinDistance(rdd1, rdd2, matchCount, matchDist, adjustForBoundCross)
            case QueryType.kNN =>
                geoMatch.spatialJoinKNN(rdd1, rdd2, matchCount, adjustForBoundCross)
            case QueryType.range =>
                geoMatch.spatialJoinRange(rdd1, rdd2, adjustForBoundCross)
        }

        rddResult.mapPartitions(_.map(x => {

            StringBuilder
                .newBuilder
                .append(x._1.payload)
                .append(',')
                .append(x._2.map(_.payload).mkString(","))
        }))
            .saveAsTextFile(clArgs.getParamValueString(GM_Arguments.outDir), classOf[GzipCodec])

        if (clArgs.getParamValueBoolean(GM_Arguments.local)) {

            println("Total Time: " + (System.currentTimeMillis() - startTime))
            println("Output idr: " + clArgs.getParamValueString(GM_Arguments.outDir))
        }
    }
}