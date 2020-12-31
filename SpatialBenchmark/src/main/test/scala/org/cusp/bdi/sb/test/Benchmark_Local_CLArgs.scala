package org.cusp.bdi.sb.test

import org.cusp.bdi.sb.test.parser.SB_KeyMatchInputFileParser_RandomPoints
import org.cusp.bdi.util.{CLArgsParser, LocalRunConsts}

object Benchmark_Local_CLArgs {

  val SKNN_RandomPoint_RandomPoint_allKnn: CLArgsParser = apply(
    LocalRunConsts.pathSparkKNN_FW_Output_1,
    SB_KeyMatchInputFileParser_RandomPoints.getClass.getName,
    LocalRunConsts.pathSparkKNN_FW_Output_2,
    SB_KeyMatchInputFileParser_RandomPoints.getClass.getName,
    "",
    10)

  val SKNN_RandomPoint_RandomPoint_knn: CLArgsParser = apply(
    LocalRunConsts.pathSparkKNN_FW_Output_1,
    SB_KeyMatchInputFileParser_RandomPoints.getClass.getName,
    LocalRunConsts.pathSparkKNN_FW_Output_2,
    SB_KeyMatchInputFileParser_RandomPoints.getClass.getName,
    "\\S*_[Aa]_\\S*",
    10)

  //    val SKNN_RandomPoint_RandomPoint = SB_CLArgs(LocalRunConsts.pathKM_RandomPointsNonUniform, SB_KeyMatchInputFileParser_RandomPoints.getClass.getName, LocalRunConsts.pathSparkKNN_FW_Output, SB_KeyMatchInputFileParser_RandomPoints.getClass.getName)
  //    val SKNN_BusPoint_BusPointShift = SB_CLArgs(LocalRunConsts.pathKM_Bus_SMALL, SB_KeyMatchInputFileParser_Bus_Small.getClass.getName, LocalRunConsts.pathSparkKNN_FW_Output, SB_KeyMatchInputFileParser_Bus_Small.getClass.getName)
  //  val GM_LionTPEP: CLArgsParser = apply(LocalRunConsts.pathKM_TPEP, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_TPEP", LocalRunConsts.pathGM_TPEP, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_TPEP")
  //  val GM_LionTaxi: CLArgsParser = apply(LocalRunConsts.pathKM_Taxi, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_Taxi", LocalRunConsts.pathGM_Taxi, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_Taxi")
  //  val GS_LionTPEP: CLArgsParser = apply(LocalRunConsts.pathKM_TPEP, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_TPEP", LocalRunConsts.pathGS_TPEP, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_TPEP")
  //  val GS_LionTaxi: CLArgsParser = apply(LocalRunConsts.pathKM_Taxi, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_Taxi", LocalRunConsts.pathGS_Taxi, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_Taxi")
  //  val LS_LionTPEP: CLArgsParser = apply(LocalRunConsts.pathKM_TPEP_WGS84, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_TPEP_wgs84", LocalRunConsts.pathLS_wgs_TPEP, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_TPEP_wgs84")
  //  val LS_LionTaxi: CLArgsParser = apply(LocalRunConsts.pathKM_Taxi_WGS84, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_Taxi_wgs84", LocalRunConsts.pathLS_wgs_Taxi, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_Taxi_wgs84")
  //  val LS_LionBus: CLArgsParser = apply(LocalRunConsts.pathKM_Bus_WGS84, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_Bus_wgs84", LocalRunConsts.pathLS_wgs_Bus, "org.cusp.bdi.sb.test.SB_KeyMatchInputFileParser_Bus_wgs84")

  private def apply(keyMatchInFile: String, kmInputFileParser: String, testFWInFile: String, testFWInFileParser: String, keyRegex: String, classificationCount: Int) =
    CLArgsParser(Arguments_Benchmark(debug = true, keyMatchInFile, kmInputFileParser, testFWInFile, testFWInFileParser, keyRegex, classificationCount), Arguments_Benchmark.lstArgInfo())
}