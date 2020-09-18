/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.util

abstract class Arguments {

    val local: (String, String, Boolean, String) = ("local", "Boolean", false, "(T=local, F=cluster)")
    val debug: (String, String, Boolean, String) = ("debug", "Boolean", false, "(T=show_debug, F=no_debug)")
    val outDir: (String, String, Null, String) = ("outDir", "String", null, "File location to write benchmark results")
    val firstSet: (String, String, Null, String) = ("firstSet", "String", null, "First data set input file path (LION Streets)")
    val firstSetObj: (String, String, Null, String) = ("firstSetObj", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")
    val secondSet: (String, String, Null, String) = ("secondSet", "String", null, "Second data set input file path (Bus, TPEP, Yellow)")
    val secondSetObj: (String, String, Null, String) = ("secondSetObj", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")
    val errorRange: (String, String, Int, String) = ("errorRange", "Double", 150, "Error range by which to adjust spacial objects")
    val matchCount: (String, String, Int, String) = ("matchCount", "Int", 3, "Number of matched geometries to keept (i.e. # points per streeet)")
    val matchDist: (String, String, Int, String) = ("matchDist", "Double", 150, "Maximum distance after which the match is discarded")

    def apply() =
        List(local, debug, outDir, firstSet, firstSetObj, secondSet, secondSetObj, errorRange, matchCount, matchDist)
}

class Arguments_QueryType extends Arguments {

    val queryType: (String, String, Null, String) = ("queryType", "String", null, "The query type (e.g. distance, kNN, range)")

    override def apply(): List[(String, String, Any, String)] =
        super.apply() ++ List(queryType)
}
