//package org.cusp.bdi.tools.convert;
//
//import org.cusp.bdi.cmn.arg.CLArgs
//import org.cusp.bdi.cmn.Helper
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.hadoop.io.compress.GzipCodec
//
//abstract class ConvertToWKT_Base extends Serializable {
//
//    def inputLineParser(arg: String): String
//
//    val ARG_NAMES = Array("inputDir", "outDir", "local", "debug")
//
//    /* Parameter name, Data Type, Description, Default value */
//    val lstReqParamInfo = List[(String, String, String, Any)](((ARG_NAMES(0), "String", "Dir containing input files", null)), //
//        ((ARG_NAMES(1), "String", "Output dir path", null)), //
//        ((ARG_NAMES(2), "Boolean", "(T=local, F=cluster)", false)), //
//        ((ARG_NAMES(3), "Boolean", "(T=show_debug, F=no_debug)", false)))
//
//    def main(args: Array[String]): Unit = {
//
//        val startTime = System.currentTimeMillis()
//
//        val clArgs = CLArgs(args, lstReqParamInfo)
//
//        val appName = clArgs.toString(this)
//        var outputDir = clArgs.getParamValueString(ARG_NAMES(1))
//        val debugOn = clArgs.getParamValueBoolean(ARG_NAMES(3))
//
//        val conf = new SparkConf().setAppName(appName)
//
//        if (clArgs.getParamValueBoolean(ARG_NAMES(2))) {
//
//            conf.setMaster("local[*]")
//
//            // randomize output dir
//            outputDir = Helper.randOutputDir(outputDir)
//        }
//
//        val sc = new SparkContext(conf)
//
//        Helper.logMessage(debugOn, this, "Spark started")
//
//        val rddInput = sc.textFile(clArgs.getParamValueString(ARG_NAMES(0)))
//            .mapPartitions(_.map(inputLineParser), true)
//            .saveAsTextFile(outputDir, classOf[GzipCodec])
//
//        if (clArgs.getParamValueBoolean(ARG_NAMES(2))) {
//
//            println("Total Time: " + (System.currentTimeMillis() - startTime))
//            println("Output idr: " + outputDir)
//        }
//    }
//}