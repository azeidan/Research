package org.cusp.bdi.sb.fw.ls

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.cusp.bdi.util.Helper
import org.cusp.bdi.util.InputFileParsers

import cs.purdue.edu.spatialindex.rtree.Box
import cs.purdue.edu.spatialindex.rtree.Point
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.Util
import org.cusp.bdi.util.CLArgsParser

//class MBRInfo(geomOrig: Geometry, mbr: Array[(Float, Float)]) extends Box(mbr.head._1, mbr.head._2, mbr.last._1, mbr.last._2) {
//    var geometryOriginal = geomOrig
//}

class BoxData(mbr: Array[(Float, Float)], _data: String) extends Box(mbr(0)._1, mbr(0)._2, mbr(1)._1, mbr(1)._2) {
    var data = _data
}

object LS_Example extends Serializable {

    private def getRDDFirstSet(objType: String, sparkContext: SparkContext, fileName: String, expandBy: Float): RDD[Box] =
        objType.toLowerCase() match {

            case s if s matches "(?i)LION_wgs_Box" =>
                sparkContext.textFile(fileName)
                    .mapPartitions(_.map(InputFileParsers.nycLION_WGS84))
                    .filter(_ != null)
                    .mapPartitions(_.map(row => {

                        val mbr = Helper.getMBREnds(row._2.map(x => (x._1.toFloat, x._2.toFloat)), expandBy)

                        new BoxData(mbr, row._1)
                    }))
        }

    private def getRDDSecondSet(objType: String, sparkContext: SparkContext, fileName: String): RDD[(Point, String)] = {

        val lineParser = objType match {
            case s if s matches "(?i)TPEP_wgs_Point" => InputFileParsers.tpepPoints_WGS84
            case s if s matches "(?i)Taxi_wgs_Point" => InputFileParsers.taxiPoints_WGS84
            case s if s matches "(?i)Bus_wgs_Point" => InputFileParsers.busPoints_WGS84
        }

        sparkContext.textFile(fileName)
            .mapPartitions(_.map(lineParser))
            .filter(_ != null)
            .mapPartitions(_.map(parts => {

                val (x, y) = (parts._2._1.toFloat, parts._2._2.toFloat)

                if (x < -180 || x > 180 || y < -90 || y > 90)
                    null
                else
                    (Point(x, y), parts._1)
            })
                .filter(_ != null))
    }

    def main(args: Array[String]): Unit = {

        val startTime = System.currentTimeMillis()

        //        val clArgs = LS_CLArgs.lion_Box_TPEP_Point
        //        val clArgs = LS_CLArgs.lion_Box_Taxi_Point
        //        val clArgs = LS_CLArgs.lion_Box_Bus_Point
        val clArgs = CLArgsParser(args, LS_Arguments())

        val conf = new SparkConf()
            .setAppName(StringBuilder
                .newBuilder
                .append("LS_Example_")
                .append("_")
                .append(clArgs.getParamValueString(LS_Arguments.queryType))
                .append("_")
                .append(clArgs.getParamValueString(LS_Arguments.firstSetObj))
                .append("_")
                .append(clArgs.getParamValueString(LS_Arguments.secondSetObj))
                .toString)

        if (clArgs.getParamValueBoolean(LS_Arguments.local))
            conf.setMaster("local[*]")

        val sparkContext = new SparkContext(conf)

        // delete output dir if exists
        Helper.delDirHDFS(sparkContext, clArgs.getParamValueString(LS_Arguments.outDir))

        val rddSecondSetRaw = getRDDSecondSet(clArgs.getParamValueString(LS_Arguments.secondSetObj), sparkContext, clArgs.getParamValueString(LS_Arguments.secondSet))

        Util.localIndex = getLocalIndex(clArgs.getParamValueChar(LS_Arguments.sIdx))

        val srddSecondSetRaw = SpatialRDD(rddSecondSetRaw)

        val rddFirstSet = getRDDFirstSet(clArgs.getParamValueString(LS_Arguments.firstSetObj), sparkContext, clArgs.getParamValueString(LS_Arguments.firstSet), clArgs.getParamValueFloat(LS_Arguments.errorRange))

        val rddJoinResult = srddSecondSetRaw.rjoin(rddFirstSet)(aggfunction1, aggfunction2)

        val matchDist = clArgs.getParamValueDouble(LS_Arguments.matchDist)

        rddJoinResult.mapPartitions(_.map(tupleBxLst => {

            val boxData = tupleBxLst._1 match { case x: BoxData => x }

            tupleBxLst._2.map(pointStrTuple => {

                val (pointGeom: Point, pointData: String) = pointStrTuple

                val dist = boxData.distance(pointGeom)

                val sb = StringBuilder.newBuilder
                    .append(pointData)
                    .append(",")

                if (dist <= matchDist)
                    sb.append(boxData.data)

                sb
            })
        }))
            .flatMap(identity)
            .saveAsTextFile(clArgs.getParamValueString(LS_Arguments.outDir), classOf[GzipCodec])

        if (clArgs.getParamValueBoolean(LS_Arguments.local)) {

            println("Total Time: " + (System.currentTimeMillis() - startTime))
            println("Output idr: " + clArgs.getParamValueString(LS_Arguments.outDir))
        }
    }

    private def getLocalIndex(sIndex: Char) = {

        sIndex.toUpper match {
            case 'G' => "grid"
            case 'I' => "irtree"
            case 'Q' => "qtree"
            case 'R' => "rtree"
            case _ => throw new Exception("Invalid grid index option (Only G, I, Q or R): " + sIndex)
        }
    }

    private def aggfunction1[K, V](itr: Iterator[(K, V)]) =
        itr

    /**
      * An aggregate function used by LocationSpark
      * This function accepts two parameter of type ListBuffer, merges them into one list
      *
      * lst1, 1st ListBuffer
      * lst2, 2nd ListBuffer
      */
    private def aggfunction2(lst1: Iterator[(Object, Object)], lst2: Iterator[(Object, Object)]) =
        lst2 ++ lst1

    //    /**
    //      * An aggregate function used by LocationSpark
    //      *
    //      * This function will convert the iterator of type (K, V) to a ListBuffer of type (K, V) where:
    //      *
    //      * K is the TLC GPS point
    //      * V is the TLC row data
    //      */
    //    private def aggfunction1[K, V](itr: Iterator[(K, V)]) =
    //        itr
    //
    //    /**
    //      * An aggregate function used by LocationSpark
    //      * This function accepts two parameter of type ListBuffer, merges them into one list
    //      */
    //    private def aggfunction2[K, V](iter1: Iterator[(K, V)], iter2: Iterator[(K, V)]) =
    //        iter1 ++ iter2

}