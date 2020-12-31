package org.cusp.bdi.sb.test

import org.cusp.bdi.util.{Arguments, Helper, LocalRunConsts}

object Arguments_Benchmark extends Serializable {

  val local: (String, String, Boolean, String) = Arguments.local
  val debug: (String, String, Boolean, String) = Arguments.debug
  val outDir: (String, String, Null, String) = Arguments.outDir
  val classificationCount: (String, String, Int, String) = Arguments.buildTuple("-classificationCount", "Int", 3, "Number of matches that can be out-of-order when classifying the output. i.e. 3 means positions 0,1,2 can appear as 1,0,2 or 2,0,1 ...")
  val keyMatchInFile: (String, String, Null, String) = Arguments.buildTuple("-keyMatchInFile", "String", null, "File location of key")
  val keyMatchInFileParser: (String, String, Null, String) = Arguments.buildTuple("-keyMatchInFileParser", "String", null, "The full class name of the key match result file specified in keyMatchInFile. Pass in the complete class name (package.className); the class should extend the class org.cusp.bdi.sb.BenchmarkInputFileParser. Reflection will be used to instantiate the class")
  val testFWInFile: (String, String, Null, String) = Arguments.buildTuple("-testFWInFile", "String", null, "Use if the framework's results should be obtained from an input file. Pass the full path of the file and specify the parser class.")
  val testFWInFileParser: (String, String, Null, String) = Arguments.buildTuple("-testFWInFileParser", "String", null, "The full class name of the framework's result file specified in testFWInFile. Pass in the complete class name (package.className); the class should extend the class org.cusp.bdi.sb.BenchmarkInputFileParser")
  val keyRegex: (String, String, String, String) = Arguments.buildTuple("-keyRegex", "String", "", "Regex to apply to the key. If the regex matches, the key is allowed")

  def lstArgInfo() =
    List(local, debug, outDir, classificationCount, keyMatchInFile, keyMatchInFileParser, testFWInFile, keyRegex, testFWInFileParser)

  def apply(debug: Boolean, keyMatchInFile: String, keyMatchInFileParser: String, testFWInFile: String, testFWInFileParser: String, keyRegex: String, classificationCount: Int): Array[String] =
    Array(Arguments_Benchmark.local._1, " T",
      Arguments_Benchmark.debug._1, Helper.toString(debug),
      Arguments_Benchmark.outDir._1, Helper.randOutputDir(LocalRunConsts.pathOutput),
      Arguments_Benchmark.classificationCount._1, classificationCount.toString,
      Arguments_Benchmark.keyMatchInFile._1, keyMatchInFile,
      Arguments_Benchmark.keyMatchInFileParser._1, keyMatchInFileParser,
      Arguments_Benchmark.testFWInFile._1, testFWInFile,
      Arguments_Benchmark.keyRegex._1, keyRegex,
      Arguments_Benchmark.testFWInFileParser._1, testFWInFileParser)
}