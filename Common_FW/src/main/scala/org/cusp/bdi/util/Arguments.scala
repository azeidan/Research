package org.cusp.bdi.util

object Arguments extends Serializable {

  def buildTuple[T](name: String, dataType: String, defaultValue: T, description: String): (String, String, T, String) =
    (name, dataType, defaultValue, description)

  val local: (String, String, Boolean, String) = buildTuple("-local", "Boolean", false, "(T=local, F=cluster)")
  val debug: (String, String, Boolean, String) = buildTuple("-debug", "Boolean", false, "(T=show_debug, F=no_debug)")
  val driverMemory: (String, String, Null, String) = Arguments.buildTuple("-driverMemory", "String", null, "value set during local mode for spark.driver.memory")
  val executorMemory: (String, String, Null, String) = Arguments.buildTuple("-executorMemory", "String", null, "value set during local mode for spark.executor.memory")
  val numExecutors: (String, String, Null, String) = Arguments.buildTuple("-numExecutors", "Int", null, "value set during local mode for spark.num.executors")
  val executorCores: (String, String, Null, String) = Arguments.buildTuple("-executorCores", "Int", null, "value set during local mode for spark.executor.cores")
  val outDir: (String, String, Null, String) = buildTuple("-outDir", "String", null, "File location to write benchmark results")
  val firstSet: (String, String, Null, String) = buildTuple("-firstSet", "String", null, "First data set input file path (LION Streets)")
  val firstSetObjType: (String, String, Null, String) = buildTuple("-firstSetObjType", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")
  val secondSet: (String, String, Null, String) = buildTuple("-secondSet", "String", null, "Second data set input file path (Bus, TPEP, Yellow)")
  val secondSetObjType: (String, String, Null, String) = buildTuple("-secondSetObjType", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")
  val k: (String, String, Int, String) = buildTuple("-k", "Int", 3, "Value of k")
  val indexType: (String, String, Null, String) = buildTuple("-indexType", "String", null, "qt for QuadTree, kdt for K-d Tree")
  val knnJoinType: (String, String, Null, String) = buildTuple("-knnJoinType", "String", null, "knn for Left knn Right, allKNN for both datasets")

  def lstArgInfo() = List(local, debug, driverMemory, executorMemory, numExecutors, executorCores, outDir, firstSet, firstSetObjType, secondSet, secondSetObjType, k, knnJoinType, indexType)

  def apply(debug: Boolean, driverMemory: String, executorMemory: String, numExecutors: Int, executorCores: Int, firstSet: String, firstSetObj: String, secondSet: String, secondSetObj: String, k: Int, joinType: String, indexType: String, other: (String, String)*): Array[String] =
    Array(Arguments.local._1, "T",
      Arguments.debug._1, Helper.toString(debug),
      Arguments.driverMemory._1, driverMemory,
      Arguments.executorMemory._1, executorMemory,
      Arguments.numExecutors._1, numExecutors.toString,
      Arguments.executorCores._1, executorCores.toString,
      Arguments.outDir._1, Helper.randOutputDir(LocalRunConsts.pathOutput),
      Arguments.firstSet._1, firstSet,
      Arguments.firstSetObjType._1, firstSetObj,
      Arguments.secondSet._1, secondSet,
      Arguments.secondSetObjType._1, secondSetObj,
      Arguments.k._1, k.toString,
      Arguments.knnJoinType._1, joinType,
      Arguments.indexType._1, indexType) ++
      other.map(row => Array(row._1, row._2.toString)).flatMap(_.seq)
}

