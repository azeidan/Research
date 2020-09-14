package spark

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.util.LocalRunConsts

object RandomSampler {

  var localMode = true
  val outDir = "/gws/projects/project-taxi_capstone_2016/share/GeoMatch_Work_Folder/RandomSamples/"
  var numRows = 5e6.toInt

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)

    if (localMode)
      sparkConf.setMaster("local[*]")
        .set("spark.local.dir", LocalRunConsts.sparkWorkDir)

    val sc = new SparkContext(sparkConf)

    def getSample(inputFile: String, parser: String => (String, String), outputFile: String, numRows: Int) {

      val rddLeft = sc.textFile(inputFile)

      sc.parallelize(rddLeft.takeSample(false, numRows))
        .mapPartitions(_.map(parser))
        .filter(_ != null)
        .groupByKey()
        .mapPartitions(_.map(_._2.head))
        .saveAsTextFile(outputFile, classOf[GzipCodec])
    }

        var inputFile = "/gws/projects/project-taxi_capstone_2016/share/Bus_TripRecod_NAD83/"
        var label = "Bus"
        var parser = (line: String) => {

          val arr = line.split(",")

          if (arr(1).toDouble.isNaN || arr(2).toDouble.isNaN)
            null
          else
            ("%.8f%.8f".format(arr(1).toDouble, arr(2).toDouble), line)
        }

//        var inputFile = "/gws/projects/project-taxi_capstone_2016/data/breadcrumb_nad83/"
//        var label = "Bread"
//        var parser = (line: String) => {
//
//          val arr = line.split(",")
//
//          if (arr(1).toDouble.isNaN || arr(2).toDouble.isNaN)
//            null
//          else
//            ("%.8f%.8f".format(arr(1).toDouble, arr(2).toDouble), line)
//        }

//    var inputFile = "/gws/projects/project-taxi_capstone_2016/share/Yellow_TLC_TripRecord_NAD83/NYCYellowTaxiProj_2014_0[1-9]/"
//    var label = "Taxi"
//    var parser = (line: String) => {
//
//      val arr = line.split(",")
//
//      if (arr(5).toDouble.isNaN || arr(6).toDouble.isNaN)
//        null
//      else
//        ("%.8f%.8f".format(arr(5).toDouble, arr(6).toDouble), line)
//    }

    Range(1, 4).foreach(i =>
      getSample(inputFile, parser, outDir + label + i, numRows))
  }
}
