/**
  * Copyright (c) 2019, The City University of New York and the University of Helsinki
  * All rights reserved.
  */

package org.cusp.bdi.util

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import util.control.Breaks._

/**
  * Helper class for parsing and retrieving command line arguments. The class parses the args array based on the argument list.
  * Error message are printed using the info in the list versus what is provided in the args array
  *
  */
case class CLArgsParser(args: Array[String], lstArgInfo: List[(String, String, Any, String)]) extends Serializable {

  //  private val SPACER = "       "
  private val mapProgramArg = mutable.HashMap[String, Any]()
  private val mapCLArgs = Map[String, String]()

  def updateParamValue(paramInfo: (String, String, Any, String), newVal: Any): CLArgsParser = {

    mapProgramArg.update(paramInfo._1.toLowerCase(), newVal)

    this
  }

  def getParamValueChar(paramInfo: (String, String, Any, String)): Char = getParamValueString(paramInfo).charAt(0)

  def getParamValueDouble(paramInfo: (String, String, Any, String)): Double = getParamValueString(paramInfo).toDouble

  def getParamValueFloat(paramInfo: (String, String, Any, String)): Float = getParamValueString(paramInfo).toFloat

  def getParamValueInt(paramInfo: (String, String, Any, String)): Int = getParamValueString(paramInfo).toInt

  def getParamValueString(paramInfo: (String, String, Any, String)): String = mapProgramArg(paramInfo._1.toLowerCase()).toString

  def getParamValueBoolean(paramInfo: (String, String, Any, String)): Boolean = getParamValueString(paramInfo).toLowerCase() match {

    case "t" | "true" | "y" => true
    case _ => false
  }

  buildArgMap()

  def buildArgMap() {

    var missingArg = args == null || args.isEmpty || args.length != lstArgInfo.size * 2

    try {
      if (missingArg)
        throw new IllegalArgumentException(buildUsageString(null))
      else {

        val mapCLArgs = args.grouped(2).map(x => (x(0).trim.toLowerCase, x(1).trim)).toMap

        //        mapCLArgs = lstArgs.grouped(2).map(arr => arr.head.toLowerCase() -> arr(1)).toMap

        lstArgInfo.foreach(argInfo => {

          val (argName, argType, argDefault) = (argInfo._1.trim.toLowerCase, argInfo._2.trim.toLowerCase, argInfo._3)

          val paramVal = mapCLArgs.get(argName)

          if (paramVal.isEmpty) {

            if (argDefault == null)
              throw new IllegalArgumentException(buildUsageString(null))

            mapProgramArg += argName -> argDefault
          }
          else
            argType match {
              case "char" =>
                val c = paramVal.get

                if (c.length() == 1)
                  mapProgramArg += argName -> c
                else {
                  mapProgramArg += argName -> (paramVal.get + "<-Err")
                  missingArg = true
                }
              case "int" =>
                try {
                  mapProgramArg += argName -> paramVal.get.toInt
                }
                catch {
                  case _: Exception =>
                    mapProgramArg += argName -> (paramVal.get + "<-Err")
                    missingArg = true
                }
              case "double" =>
                try {
                  mapProgramArg += argName -> paramVal.get.toDouble
                }
                catch {
                  case _: Exception =>
                    mapProgramArg += argName -> (paramVal.get + "<-Err")
                    missingArg = true
                }
              case "boolean" =>
                try {
                  mapProgramArg += argName -> Helper.toBoolean(paramVal.get)
                }
                catch {
                  case _: Exception =>
                    mapProgramArg += argName -> (paramVal.get + "<-Err")
                    missingArg = true
                }
              case _: String =>
                mapProgramArg += argName -> paramVal.get
              case _ =>
                missingArg = true
            }
        })
      }

      if (mapProgramArg.isEmpty || missingArg)
        throw new IllegalArgumentException(buildUsageString(mapProgramArg))
    }
    catch {
      case ex: Exception =>

        //        ex.printStackTrace()

        throw new Exception("%s %d out of the expected %d%n%s%n%n%s".format("Number of args received", args.length, lstArgInfo.size * 2, args.mkString(" "), ex.toString))
    }
  }

  def buildUsageString(mapProgramArg: mutable.HashMap[String, Any]): String =
    "%s%nAllowed Arguments: %n%s%s".format("Missing parameters or incorrect value types.", this.toString, if (mapProgramArg == null) "" else mapProgramArg.mkString("\t\n"))

  override def toString: String =
    lstArgInfo.map(t => {

      val (key, valType, valDefault, valDesc) = (t._1, t._2.toLowerCase(), t._3, t._4)

      var valueState = ""
      if (mapCLArgs == null || !mapCLArgs.contains("-" + key.toLowerCase()))
        if (valDefault == null)
          valueState = "NOT passed as argument"
        else
          valueState = "defaulted to: " + t._3
      else
        valueState = "passed as arguments: " + mapProgramArg(key.toLowerCase())

      "\t-%s, Type: %s, Required: %s, Value %s, Desc: %s%n".format(key, valType, if (valDefault == null) "Yes" else "No", valueState, valDesc)
    })
      .mkString("")
}

object Test_CLArgsParser {

  def main(args: Array[String]): Unit = {

    val lstReqParamInfo = List[(String, String, Any, String)](("local", "Boolean", false, "(T=local, F=cluster)"),
      ("debug", "Boolean", false, "(T=show_debug, F=no_debug)"),
      ("outDir", "String", null, "File location to write benchmark results"),
      ("firstSet", "String", null, "First data set input file path (LION Streets)"),
      ("firstSetObj", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)"),
      ("secondSet", "String", null, "Second data set input file path (Bus, TPEP, Yellow)"),
      ("secondSetObj", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)"),
      ("queryType", "String", null, "The query type (e.g. distance, kNN, range)"),
      ("hilbertN", "Int", 256, "The size of the hilbert curve (i.e. n)"),
      ("errorRange", "Double", 150, "Value by which to expand each MBR prior to adding it to the RTree"),
      ("matchCount", "Int", 3, "Number of matched geometries to keept (i.e. # points per streeet)"),
      ("matchDist", "Double", 150, "Maximum distance after which the match is discarded"),
      ("searchGridMinX", "Int", -1, "Search grid's minimum X"),
      ("searchGridMinY", "Int", -1, "Search grid's minimum Y"),
      ("searchGridMaxX", "Int", -1, "Search grid's maximum X"),
      ("searchGridMaxY", "Int", -1, "Search grid's maximum Y"))

    println(CLArgsParser(args, lstReqParamInfo))
  }
}