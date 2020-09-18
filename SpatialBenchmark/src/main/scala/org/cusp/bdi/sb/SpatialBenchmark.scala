package org.cusp.bdi.sb

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.sb.examples.{Arguments_Benchmark, BenchmarkInputFileParser, Benchmark_Local_CLArgs}
import org.cusp.bdi.util.{CLArgsParser, LocalRunConsts}

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
//            val clArgs = Benchmark_Local_CLArgs.SKNN_RandomPoint_RandomPoint
    val clArgs = CLArgsParser(args, Arguments_Benchmark.lstArgInfo())

    val keyMatchInFileParser = instantiateClass[BenchmarkInputFileParser](clArgs.getParamValueString(Arguments_Benchmark.keyMatchInFileParser))
    val testFWInFileParser = instantiateClass[BenchmarkInputFileParser](clArgs.getParamValueString(Arguments_Benchmark.testFWInFileParser))

    val sparkConf = new SparkConf().setAppName("Spatial Benchmark")

    if (clArgs.getParamValueBoolean(Arguments_Benchmark.local))
      sparkConf.setMaster("local[*]")

    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName)
    sparkConf.registerKryoClasses(Array(classOf[String]))

    val sparkContext = new SparkContext(sparkConf)

    // delete output dir if exists
    val hdfs = FileSystem.get(sparkContext.hadoopConfiguration)
    val path = new Path(clArgs.getParamValueString(Arguments_Benchmark.outDir))
    if (hdfs.exists(path))
      hdfs.delete(path, true)

    val rddKeyMatch = sparkContext.textFile(clArgs.getParamValueString(Arguments_Benchmark.keyMatchInFile))

    val rddTestFW = sparkContext.textFile(clArgs.getParamValueString(Arguments_Benchmark.testFWInFile))

    val compareResults = OutputsCompare(clArgs.getParamValueInt(Arguments_Benchmark.classificationCount), rddKeyMatch, keyMatchInFileParser, rddTestFW, testFWInFileParser)

    compareResults.append("Total Runtime: " + "%,d".format(System.currentTimeMillis() - startTime) + " ms")

    sparkContext.parallelize(compareResults, 1)
      .saveAsTextFile(clArgs.getParamValueString(Arguments_Benchmark.outDir), classOf[GzipCodec])

    if (clArgs.getParamValueBoolean(Arguments_Benchmark.local)) {

      LocalRunConsts.logLocalRunEntry(LocalRunConsts.benchmarkLogFile, "sKNN",
        clArgs.getParamValueString(Arguments_Benchmark.keyMatchInFile).substring(clArgs.getParamValueString(Arguments_Benchmark.keyMatchInFile).lastIndexOf("/") + 1),
        clArgs.getParamValueString(Arguments_Benchmark.testFWInFile).substring(clArgs.getParamValueString(Arguments_Benchmark.testFWInFile).lastIndexOf("/") + 1),
        compareResults.mkString("\n"),
        (System.currentTimeMillis() - startTime) / 1000.0)

      println("Output idr: " + clArgs.getParamValueString(Arguments_Benchmark.outDir))
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