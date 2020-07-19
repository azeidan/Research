//import org.cusp.bdi.cmn.arg.CLArgs
//import org.cusp.bdi.cmn.Helper
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.hadoop.io.compress.GzipCodec
//import org.cusp.bdi.tools.InputFileParsers
//import scala.collection.mutable.ListBuffer
//
//object ExtractMBR extends Serializable {
//
//    val ARG_NAMES = Array("inputFileFormat", "inputFile", "outDir", "local", "debug")
//
//    /* Parameter name, Data Type, Description, Default value */
//    val lstReqParamInfo = List[(String, String, String, Any)](((ARG_NAMES(0), "String", "The format of the input file. i.e. nycLion, Bus, TPEP, or Yellow", null)), //
//        ((ARG_NAMES(1), "String", "Input file(s)", null)), //
//        ((ARG_NAMES(2), "String", "Output dir path", null)), //
//        ((ARG_NAMES(3), "Boolean", "(T=local, F=cluster)", false)), //
//        ((ARG_NAMES(4), "Boolean", "(T=show_debug, F=no_debug)", false)))
//
//    def main(args: Array[String]): Unit = {
//
//        val startTime = System.currentTimeMillis()
//
//        //        val arguments = "-local T -debug T -inputFileFormat bus -inputFile /media/cusp/Data/GeoMatch_Files/InputFiles/Bus_part-00002.1K.csv -outDir /media/cusp/Data/GeoMatch_Files/OutputFiles/"
//        //        val arguments = "-local T -debug T -inputFileFormat nycLION -inputFile /media/cusp/Data/GeoMatch_Files/InputFiles/LION_NYC_Streets_NAD83_Enumerated.csv -outDir /media/cusp/Data/GeoMatch_Files/OutputFiles/"
//        //        val clArgs = CLArgs(arguments.split(" "), lstReqParamInfo)
//
//        //                val arguments = "-inputFileFormat bus -inputFile /gws/projects/project-taxi_capstone_2016/share/Bus_TripRecod_NAD83/*.gz -debug F -local T -outDir /gws/projects/project-taxi_capstone_2016/share/_Spark_Output/DataSetTools/ExtractBusMBR_50x5"
//        //                val clArgs = CLArgs(arguments.split(" "), lstReqParamInfo)
//
//        val clArgs = CLArgs(args, lstReqParamInfo)
//
//        val appName = clArgs.toString(this)
//        var outputDir = clArgs.getParamValueString(ARG_NAMES(2))
//        val debugOn = clArgs.getParamValueBoolean(ARG_NAMES(3))
//
//        val conf = new SparkConf().setAppName(appName)
//
//        val inputLineParser = clArgs.getParamValueString(ARG_NAMES(0)).toLowerCase match {
//            case "bus" => InputFileParsers.busPoints
//            case "nyclion" => InputFileParsers.nycLION
//            case _ => throw new Exception("Unsupported file format")
//        }
//
//        if (clArgs.getParamValueBoolean(ARG_NAMES(3))) {
//
//            conf.setMaster("local[*]")
//
//            outputDir = Helper.randOutputDir(outputDir)
//        }
//
//        val sc = new SparkContext(conf)
//
//        Helper.logMessage(debugOn, this, "Spark started")
//
//        val rddInput = sc.textFile(clArgs.getParamValueString(ARG_NAMES(1)))
//            .mapPartitions(_.map(inputLineParser)
//                .filter(_ != null), true)
//            .mapPartitions(_.map(xy => computeMBR(xy ++ xy)), true)
//            .saveAsTextFile(outputDir, classOf[GzipCodec])
//
//        if (clArgs.getParamValueBoolean(ARG_NAMES(3))) {
//
//            println("Total Time: " + (System.currentTimeMillis() - startTime))
//            println("Output idr: " + outputDir)
//        }
//    }
//
//    def computeMBR(xyArr: Array[(Double, Double)]) = {
//
//        var xStart = xyArr(0)
//        var yStart = xyArr(1)
//        val sb = StringBuilder.newBuilder
//
//        (1 until xyArr.length / 2).foreach(i => {
//
//            val xEnd = xyArr(i * 2)
//            val yEnd = xyArr(i * 2 + 1)
//
//            val mbr = Helper.getMBR((xStart, yStart), (xEnd, yEnd))
//
//            sb.append(mbr._1._1 + "," + mbr._1._2 + "," + mbr._2._1 + "," + mbr._2._2 + "\n")
//
//            xStart = xEnd
//            yStart = xEnd
//        })
//
//        sb.substring(0, sb.length - 1)
//    }
//}