package org.cusp.bdi.tools

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WGS_Distance_Test_Analysis {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
            .setAppName(this.getClass.getName)
            .setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        def foldOp(x: (Double, Double, Double, Long), y: (Double, Double, Double, Long)) = {

            var min = x._1
            var max = x._2

            if (y._1 < min)
                min = y._1
            if (y._2 > max)
                max = y._2

            (min, max, x._3 + y._3, x._4 + y._4)
        }

        //        val sum = sc.textFile("/gws/projects/project-taxi_capstone_2016/share/GeoMatch_Work_Folder/150ftTestForWGS/part*.gz")
        //            .mapPartitions(_.map(line => {
        //
        //                (line.toDouble, line.toDouble, line.toDouble, 1L)
        //            }))
        //            .mapPartitions(iter => {
        //
        //                val res = iter.fold((Double.MaxValue, Double.MinValue, 0.0, 0L))(foldOp)
        //
        //                Array(res).iterator
        //            })
        //            .fold((Double.MaxValue, Double.MinValue, 0.0, 0L))(foldOp)
        //
        //        println("Min Distance: %,.7f%nMax Distance: %,.7f%nSum: %,.20f%nCount: %,d%nAvg: %,.20f%n".format(sum._1, sum._2, sum._3, sum._4, sum._3 / sum._4))
    }
}