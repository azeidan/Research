package spark

import org.apache.spark.{SparkConf, SparkContext}

object DatasetSummaryForHeatMap {

  def main(args: Array[String]): Unit = {

    def getXY(line: String, startCommaNum: Int) = {

      def getCommaPos(commaNum: Int, startIdx: Int) = {

        var count = 0
        var idx = startIdx

        while (count < commaNum) {

          if (line(idx) == ',')
            count += 1

          idx += 1
        }

        idx
      }

      try {

        val idx0 = getCommaPos(startCommaNum, 0)
        val idx1 = getCommaPos(1, idx0 + 1)
        val idx2 = getCommaPos(1, idx1 + 1)

        val x = line.substring(idx0, idx1 - 1)
        val y = line.substring(idx1, idx2 - 1)

        if (x(0) != '-' && (x(0) < '0' || x(0) > '9'))
          null
        else
          (x, y)
      }
      catch {
        case ex: Exception =>

          ex.printStackTrace()
          null
      }
    }

        val input = "/gws/projects/project-taxi_capstone_2016/data/breadcrumb_nad83"
        val output = "/gws/projects/project-taxi_capstone_2016/share/sparkKNN_WorkFolder/TPEP_Counts/"
//    val input = "/gws/projects/project-taxi_capstone_2016/share/Bus_TripRecod_NAD83"
//    val output = "/gws/projects/project-taxi_capstone_2016/share/sparkKNN_WorkFolder/Bus_Counts/"
//    val input = "/gws/projects/project-taxi_capstone_2016/share/Yellow_TLC_TripRecord_NAD83/NYCYellowTaxiProj_2014_*/part*.gz"
//    val output = "/gws/projects/project-taxi_capstone_2016/share/sparkKNN_WorkFolder/Taxi_Counts/"

    //    val parser = (line: String) => {
    //
    //      val xy = getXY(line, 2 /*, false*/)
    //
    //      if (xy == null)
    //        null
    //      else
    //        (line.toLowerCase, xy)
    //    }

    val parser: String => (String, (String, String)) = (line: String) => {

      val xy = getXY(line, 5)

      if (xy == null || xy._1.contains('-') || xy._2.contains('-'))
        null
      else
        (line.toLowerCase, xy)
    }

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    import org.apache.hadoop.io.compress.GzipCodec

    sc.textFile(input).mapPartitions(_.map(parser)).filter(_ != null).mapPartitions(_.map(row => (("%.0f".format(row._2._1.toFloat), "%.0f".format(row._2._2.toFloat)), 1L))).reduceByKey(_ + _).mapPartitions(_.map(row => "%s,%s,%d".format(row._1._1, row._1._2, row._2))).saveAsTextFile(output, classOf[GzipCodec])
  }
}
