package org.cusp.bdi.tools

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.cusp.bdi.util.Helper
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.serializer.KryoSerializer
import org.cusp.bdi.util.LocalRunConsts
import org.cusp.bdi.util.InputFileParsers

object ExtractTaxiCoords {
    def main(args: Array[String]): Unit = {

        val startTime = System.currentTimeMillis()
        var startTime2 = startTime

        //                val clArgs = SparkKNN_Local_CLArgs.randomPoints_randomPoints(SparkKNN_Arguments())
        //        val clArgs = SparkKNN_Local_CLArgs.taxi_taxi_1M(SparkKNN_Arguments())
        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift(SparkKNN_Arguments())
        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint(SparkKNN_Arguments())
        //        val clArgs = SparkKNN_Local_CLArgs.tpepPoint_tpepPoint(SparkKNN_Arguments())
        //        val clArgs = CLArgsParser(args, SparkKNN_Arguments())

        val outDir = Helper.randOutputDir(LocalRunConsts.pathOutput)

        val sparkConf = new SparkConf()
            .setAppName(this.getClass.getName)
            .set("spark.serializer", classOf[KryoSerializer].getName)
        //            .registerKryoClasses(GeoMatch.getGeoMatchClasses())

        sparkConf.setMaster("local[*]")
            .set("spark.local.dir", "/media/cusp/Data/GeoMatch_Files/spark_work_dir/")

        val sc = new SparkContext(sparkConf)

        sc.textFile("/media/ayman/Data/GeoMatch_Files/InputFiles/Yellow_TLC_TripRecord_NAD83_1M.csv")
            .mapPartitions(_.map(InputFileParsers.taxiPoints))
            .filter(_ != null)
            .mapPartitions(_.map(_._2))
            .zipWithIndex()
            .mapPartitions(_.map(row => "Taxi_%d,%s,%s".format(row._2, row._1._1, row._1._2)))
            .saveAsTextFile(outDir, classOf[GzipCodec])

        printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)

        println(outDir)
    }
}

