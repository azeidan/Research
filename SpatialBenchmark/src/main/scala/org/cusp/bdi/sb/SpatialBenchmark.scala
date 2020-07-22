package org.cusp.bdi.sb

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer
import org.cusp.bdi.sb.examples.BenchmarkInputFileParser
import org.cusp.bdi.util.CLArgsParser
import org.cusp.bdi.util.Helper
import org.cusp.bdi.sb.examples.SB_Arguments
import org.cusp.bdi.sb.examples.SB_CLArgs

object SpatialBenchmark extends Serializable {

//    private val LOGGER = LogFactory.getLog(this.getClass())

    def main(args: Array[String]): Unit = {

        val startTime = System.currentTimeMillis()

        //        val clArgs = SB_CLArgs.GM_LionTPEP
        //        val clArgs = SB_CLArgs.GM_LionTaxi
        //        val clArgs = SB_CLArgs.GS_LionTPEP
        //        val clArgs = SB_CLArgs.GS_LionTaxi
        //        val clArgs = SB_CLArgs.LS_LionTaxi
        //        val clArgs = SB_CLArgs.LS_LionBus
        //        val clArgs = SB_CLArgs.LS_LionTPEP
        //        val clArgs = SB_CLArgs.SKNN_BusPoint_BusPointShift
        val clArgs = SB_CLArgs.SKNN_RandomPoint_RandomPoint
        //        val clArgs = CLArgsParser(args, SB_Arguments())

        val keyMatchInFileParser = instantiateClass[BenchmarkInputFileParser](clArgs.getParamValueString(SB_Arguments.keyMatchInFileParser))
        val testFWInFileParser = instantiateClass[BenchmarkInputFileParser](clArgs.getParamValueString(SB_Arguments.testFWInFileParser))

        val sparkConf = new SparkConf().setAppName("Spatial Benchmark")

        if (clArgs.getParamValueBoolean(SB_Arguments.local))
            sparkConf.setMaster("local[*]")

        sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
        sparkConf.registerKryoClasses(Array(classOf[String]))

        val sparkContext = new SparkContext(sparkConf)

        // delete output dir if exists
        Helper.delDirHDFS(sparkContext, clArgs.getParamValueString(SB_Arguments.outDir))

        val rddKeyMatch = sparkContext.textFile(clArgs.getParamValueString(SB_Arguments.keyMatchInFile))

        val rddTestFW = sparkContext.textFile(clArgs.getParamValueString(SB_Arguments.testFWInFile))

        val compareResults = OutputsCompare(clArgs.getParamValueInt(SB_Arguments.classificationCount), rddKeyMatch, keyMatchInFileParser, rddTestFW, testFWInFileParser)

        compareResults.append("Total Runtime: " + "%,d".format(System.currentTimeMillis() - startTime) + " ms")

        sparkContext.parallelize(compareResults, 1)
            .saveAsTextFile(clArgs.getParamValueString(SB_Arguments.outDir), classOf[GzipCodec])

        if (clArgs.getParamValueBoolean(SB_Arguments.local)) {

            println("Output idr: " + clArgs.getParamValueString(SB_Arguments.outDir))
            compareResults.foreach(println)
        }
    }

    def instantiateClass[T](className: String): T = {

        var loadClass = className

        if (className.endsWith("$"))
            loadClass = className.substring(0, className.length() - 1)

        Class.forName(loadClass).getConstructor().newInstance().asInstanceOf[T]
    }
}