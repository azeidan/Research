package org.cusp.bdi.sknn.util

object SparkKNN_Arguments {

  val local: (String, String, Boolean, String) = ("local", "Boolean", false, "(T=local, F=cluster)")
  val debug: (String, String, Boolean, String) = ("debug", "Boolean", false, "(T=show_debug, F=no_debug)")
  val outDir: (String, String, Null, String) = ("outDir", "String", null, "File location to write benchmark results")
  val firstSet: (String, String, Null, String) = ("firstSet", "String", null, "First data set input file path (LION Streets)")
  val firstSetObjType: (String, String, Null, String) = ("firstSetObjType", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")
  val secondSet: (String, String, Null, String) = ("secondSet", "String", null, "Second data set input file path (Bus, TPEP, Yellow)")
  val secondSetObjType: (String, String, Null, String) = ("secondSetObjType", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")
  val minPartitions: (String, String, Int, String) = ("minPartitions", "Int", 0, "Suggested minimum number of partitions for the resulting RDD. 0 to leave it up to  Spark")
  val k: (String, String, Int, String) = ("k", "Int", 3, "Value of k")

  def apply() =
    List(local, debug, outDir, firstSet, firstSetObjType, secondSet, secondSetObjType, minPartitions, k)
}