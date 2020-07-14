package org.cusp.bdi.sknn.util

object SparkKNN_Arguments {

    val local = ("local", "Boolean", false, "(T=local, F=cluster)")
    val debug = ("debug", "Boolean", false, "(T=show_debug, F=no_debug)")
    val outDir = ("outDir", "String", null, "File location to write benchmark results")
    val firstSet = ("firstSet", "String", null, "First data set input file path (LION Streets)")
    val firstSetObjType = ("firstSetObjType", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")
    val secondSet = ("secondSet", "String", null, "Second data set input file path (Bus, TPEP, Yellow)")
    val minPartitions = ("minPartitions", "Int", 0, "Suggested minimum number of partitions for the resulting RDD. 0 to leave it up to  Spark")
    val k = ("k", "Int", 3, "Value of k")

    val secondSetObjType = ("secondSetObjType", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")

    def apply() =
        List(local, debug, outDir, firstSet, firstSetObjType, secondSet, secondSetObjType, minPartitions, k)
}