package org.cusp.bdi.sb.examples

import org.cusp.bdi.util.{Arguments, CLArgsParser, Helper, LocalRunConsts}

object Arguments_Benchmark extends Serializable {

  val local: (String, String, Boolean, String) = Arguments.local
  val debug: (String, String, Boolean, String) = Arguments.debug
  val outDir: (String, String, Null, String) = Arguments.outDir
  val classificationCount: (String, String, Int, String) = Arguments.buildTuple("-classificationCount", "Int", 3, "Number of matches that can be out-of-order when classifying the output. i.e. 3 means positions 0,1,2 can appear as 1,0,2 or 2,0,1 ...")
  val keyMatchInFile: (String, String, Null, String) = Arguments.buildTuple("-keyMatchInFile", "String", null, "File location of key")
  val keyMatchInFileParser: (String, String, Null, String) = Arguments.buildTuple("-keyMatchInFileParser", "String", null, "The full class name of the key match result file specified in keyMatchInFile. Pass in the complete class name (package.className); the class should extend the class org.cusp.bdi.sb.BenchmarkInputFileParser. Reflection will be used to instantiate the class")
  val testFWInFile: (String, String, Null, String) = Arguments.buildTuple("-testFWInFile", "String", null, "Use if the framework's results should be obtained from an input file. Pass the full path of the file and specify the parser class.")
  val testFWInFileParser: (String, String, Null, String) = Arguments.buildTuple("-testFWInFileParser", "String", null, "The full class name of the framework's result file specified in testFWInFile. Pass in the complete class name (package.className); the class should extend the class org.cusp.bdi.sb.BenchmarkInputFileParser")

  def lstArgInfo() =
    List(Arguments.local, Arguments.debug, Arguments.outDir, classificationCount, keyMatchInFile, keyMatchInFileParser, testFWInFile, testFWInFileParser)

  def apply(debug: Boolean, keyMatchInFile: String, keyMatchInFileParser: String, testFWInFile: String, testFWInFileParser: String, classificationCount: Int): Array[String] =
    Array(Arguments_Benchmark.local._1, " T",
      Arguments_Benchmark.debug._1, Helper.toString(debug),
      Arguments_Benchmark.outDir._1, Helper.randOutputDir(LocalRunConsts.pathOutput),
      Arguments_Benchmark.classificationCount._1, classificationCount.toString,
      Arguments_Benchmark.keyMatchInFile._1, keyMatchInFile,
      Arguments_Benchmark.keyMatchInFileParser._1, keyMatchInFileParser,
      Arguments_Benchmark.testFWInFile._1, testFWInFile,
      Arguments_Benchmark.testFWInFileParser._1, testFWInFileParser)
}

object Benchmark_Local_CLArgs {

  /* Key(Param name), Type, Default value, Description */

  val SKNN_RandomPoint_RandomPoint: CLArgsParser = apply(
    LocalRunConsts.pathSparkKNN_FW_Output_1,
    SB_KeyMatchInputFileParser_RandomPoints.getClass.getName,
    LocalRunConsts.pathSparkKNN_FW_Output_2,
    SB_KeyMatchInputFileParser_RandomPoints.getClass.getName,
    10)

  //    val SKNN_RandomPoint_RandomPoint = SB_CLArgs(LocalRunConsts.pathKM_RandomPointsNonUniform, SB_KeyMatchInputFileParser_RandomPoints.getClass.getName, LocalRunConsts.pathSparkKNN_FW_Output, SB_KeyMatchInputFileParser_RandomPoints.getClass.getName)
  //    val SKNN_BusPoint_BusPointShift = SB_CLArgs(LocalRunConsts.pathKM_Bus_SMALL, SB_KeyMatchInputFileParser_Bus_Small.getClass.getName, LocalRunConsts.pathSparkKNN_FW_Output, SB_KeyMatchInputFileParser_Bus_Small.getClass.getName)
  //  val GM_LionTPEP: CLArgsParser = apply(LocalRunConsts.pathKM_TPEP, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_TPEP", LocalRunConsts.pathGM_TPEP, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_TPEP")
  //  val GM_LionTaxi: CLArgsParser = apply(LocalRunConsts.pathKM_Taxi, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_Taxi", LocalRunConsts.pathGM_Taxi, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_Taxi")
  //  val GS_LionTPEP: CLArgsParser = apply(LocalRunConsts.pathKM_TPEP, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_TPEP", LocalRunConsts.pathGS_TPEP, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_TPEP")
  //  val GS_LionTaxi: CLArgsParser = apply(LocalRunConsts.pathKM_Taxi, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_Taxi", LocalRunConsts.pathGS_Taxi, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_Taxi")
  //  val LS_LionTPEP: CLArgsParser = apply(LocalRunConsts.pathKM_TPEP_WGS84, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_TPEP_wgs84", LocalRunConsts.pathLS_wgs_TPEP, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_TPEP_wgs84")
  //  val LS_LionTaxi: CLArgsParser = apply(LocalRunConsts.pathKM_Taxi_WGS84, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_Taxi_wgs84", LocalRunConsts.pathLS_wgs_Taxi, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_Taxi_wgs84")
  //  val LS_LionBus: CLArgsParser = apply(LocalRunConsts.pathKM_Bus_WGS84, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_Bus_wgs84", LocalRunConsts.pathLS_wgs_Bus, "org.cusp.bdi.sb.examples.SB_KeyMatchInputFileParser_Bus_wgs84")

  private def apply(keyMatchInFile: String, kmInputFileParser: String, testFWInFile: String, testFWInFileParser: String, classificationCount: Int) =
    CLArgsParser(Arguments_Benchmark(debug = true, keyMatchInFile, kmInputFileParser, testFWInFile, testFWInFileParser, classificationCount), Arguments_Benchmark.lstArgInfo())
}