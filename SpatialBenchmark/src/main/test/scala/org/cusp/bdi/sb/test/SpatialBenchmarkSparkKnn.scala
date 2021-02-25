package org.cusp.bdi.sb.test

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.util.{CLArgsParser, LocalRunConsts}

object SpatialBenchmarkSparkKnn extends Serializable {

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
//    val clArgs = Benchmark_Local_CLArgs.SKNN_RandomPoint_RandomPoint_knn
            val clArgs = CLArgsParser(args, Arguments_Benchmark.lstArgInfo())

    val sparkConf = new SparkConf().setAppName("Spatial Benchmark")

    if (clArgs.getParamValueBoolean(Arguments_Benchmark.local))
      sparkConf.setMaster("local[*]")

    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    sparkConf.registerKryoClasses(Array(classOf[String]))

    val sparkContext = new SparkContext(sparkConf)

    // delete output dir if exists
    val hdfs = FileSystem.get(sparkContext.hadoopConfiguration)
    val outDir = clArgs.getParamValueString(Arguments_Benchmark.outDir)
    val path = new Path(outDir)

    if (hdfs.exists(path))
      hdfs.delete(path, true)

    val keyMatchInFile = clArgs.getParamValueString(Arguments_Benchmark.keyMatchInFile)
    val testFWInFile = clArgs.getParamValueString(Arguments_Benchmark.testFWInFile)

    val debugMode = clArgs.getParamValueBoolean(Arguments_Benchmark.debug)
    val classificationCount = clArgs.getParamValueInt(Arguments_Benchmark.classificationCount)
    val keyMatchInFileParser = clArgs.getParamValueString(Arguments_Benchmark.keyMatchInFileParser)
    val testFWInFileParser = clArgs.getParamValueString(Arguments_Benchmark.testFWInFileParser)
    val keyRegex = clArgs.getParamValueString(Arguments_Benchmark.keyRegex)

    var lstCompareResults = OutputsCompare(debugMode, classificationCount, sparkContext.textFile(keyMatchInFile), keyMatchInFileParser, sparkContext.textFile(testFWInFile), testFWInFileParser, keyRegex)

    val runtime = (System.currentTimeMillis() - startTime) / 1000.0

    lstCompareResults ++= List("Total Runtime: %,.2f Sec".format(runtime))

    sparkContext.parallelize(lstCompareResults, 1)
      .saveAsTextFile(outDir, classOf[GzipCodec])

    if (clArgs.getParamValueBoolean(Arguments_Benchmark.local)) {

      val message = "%s\t%s\t%s\t%.4f%n\t%s%n%n".format(outDir.substring(outDir.lastIndexOf("/") + 1),
        keyMatchInFile.substring(keyMatchInFile.lastIndexOf("/") + 1),
        testFWInFile.substring(testFWInFile.lastIndexOf("/") + 1),
        runtime,
        lstCompareResults.mkString("\n"))

      LocalRunConsts.logLocalRunEntry(LocalRunConsts.benchmarkLogFile, message, null, null)

      println("Output dir: " + outDir)
      lstCompareResults.foreach(println)
    }
  }
}