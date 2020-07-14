/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.util

abstract class Arguments {

    val local = ("local", "Boolean", false, "(T=local, F=cluster)")
    val debug = ("debug", "Boolean", false, "(T=show_debug, F=no_debug)")
    val outDir = ("outDir", "String", null, "File location to write benchmark results")
    val firstSet = ("firstSet", "String", null, "First data set input file path (LION Streets)")
    val firstSetObj = ("firstSetObj", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")
    val secondSet = ("secondSet", "String", null, "Second data set input file path (Bus, TPEP, Yellow)")
    val secondSetObj = ("secondSetObj", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")
    val errorRange = ("errorRange", "Double", 150, "Error range by which to adjust spacial objects")
    val matchCount = ("matchCount", "Int", 3, "Number of matched geometries to keept (i.e. # points per streeet)")
    val matchDist = ("matchDist", "Double", 150, "Maximum distance after which the match is discarded")

    def apply() =
        List(local, debug, outDir, firstSet, firstSetObj, secondSet, secondSetObj, errorRange, matchCount, matchDist)
}

class Arguments_QueryType extends Arguments {

    val queryType = ("queryType", "String", null, "The query type (e.g. distance, kNN, range)")

    override def apply() =
        super.apply() ++ List(queryType)
}
