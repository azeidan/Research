package org.cusp.bdi.sb.test

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.util.{CLArgsParser, LocalRunConsts}

object SpatialBenchmarkSpark_kNN extends Serializable {

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
//                    val clArgs = Benchmark_Local_CLArgs.SKNN_RandomPoin t_RandomPoint
    val clArgs = CLArgsParser(args, Arguments_Benchmark.lstArgInfo())

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

    val lstCompareResults = OutputsCompare(clArgs.getParamValueInt(Arguments_Benchmark.classificationCount),
      rddKeyMatch, clArgs.getParamValueString(Arguments_Benchmark.keyMatchInFileParser),
      rddTestFW, clArgs.getParamValueString(Arguments_Benchmark.testFWInFileParser))
      .compare()

    lstCompareResults.append("Total Runtime: " + "%,d".format(System.currentTimeMillis() - startTime) + " ms")

    sparkContext.parallelize(lstCompareResults, 1)
      .saveAsTextFile(clArgs.getParamValueString(Arguments_Benchmark.outDir), classOf[GzipCodec])

    if (clArgs.getParamValueBoolean(Arguments_Benchmark.local)) {

      LocalRunConsts.logLocalRunEntry(LocalRunConsts.benchmarkLogFile, "sKNN",
        clArgs.getParamValueString(Arguments_Benchmark.keyMatchInFile).substring(clArgs.getParamValueString(Arguments_Benchmark.keyMatchInFile).lastIndexOf("/") + 1),
        clArgs.getParamValueString(Arguments_Benchmark.testFWInFile).substring(clArgs.getParamValueString(Arguments_Benchmark.testFWInFile).lastIndexOf("/") + 1),
        lstCompareResults.mkString("\n"),
        (System.currentTimeMillis() - startTime) / 1000.0)

      println("Output idr: " + clArgs.getParamValueString(Arguments_Benchmark.outDir))
      lstCompareResults.foreach(println)
    }
  }
}