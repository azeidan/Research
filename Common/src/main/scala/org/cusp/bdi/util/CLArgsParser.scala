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
  private val mapCLArgs = mutable.Map[String, String]()

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

    var missingArg = args == null || args.isEmpty

    if (!missingArg) {

      mapCLArgs ++= args.grouped(2).map(x => (x.head.trim.toLowerCase, x.last.trim))

      lstArgInfo.foreach(argInfo => {

        val (argName, argType, argDefault) = (argInfo._1.trim.toLowerCase, argInfo._2.trim.toLowerCase, argInfo._3)

        val paramVal = mapCLArgs.get(argName)

        try {
          if (paramVal.isEmpty) {

            if (argDefault == null)
              throw new IllegalArgumentException()

            mapProgramArg += argName -> argDefault
          }
          else
            argType match {
              case "char" =>
                mapProgramArg += argName -> paramVal.get.asInstanceOf[Char]
              case "int" =>
                mapProgramArg += argName -> paramVal.get.toInt
              case "double" =>
                mapProgramArg += argName -> paramVal.get.toDouble
              case "boolean" =>
                mapProgramArg += argName -> Helper.toBoolean(paramVal.get)
              case _: String =>
                mapProgramArg += argName -> paramVal.get
              case _ =>
                missingArg = true
            }
        }
        catch {
          case _: Exception =>

            mapProgramArg += argName -> (argName + "<-Err")
            missingArg = true
        }
      })
    }

    if (mapProgramArg.isEmpty || missingArg)
      throw new IllegalArgumentException(buildUsageString(mapProgramArg))

    //    throw new Exception("%s %d out of the expected %d%n%s%n%n%s".format("Number of args received", args.length, lstArgInfo.length * 2, args.mkString(" "), ex.toString))
  }

  def buildUsageString(mapProgramArg: mutable.HashMap[String, Any]): String =
    "Missing parameters or incorrect value types.%nArgument Info: %n%s".format(this.toString)

  override def toString: String =
    lstArgInfo.map(t => {

      val (key, valType, valDefault, valDesc) = (t._1, t._2, t._3, t._4)

      var valueState = ""

      if (mapCLArgs == null || !mapCLArgs.contains(key.toLowerCase()))
        if (valDefault == null)
          valueState = "NOT passed as argument"
        else
          valueState = "defaulted to: " + t._3
      else
        valueState = "extracted from args: " + mapProgramArg(key.toLowerCase())

      "\t%s, Type: %s, Required: %s, Value %s, Desc: %s%n".format(key, valType, if (valDefault == null) "Yes" else "No", valueState, valDesc)
    })
      .mkString("")
}

object Test_CLArgsParser {

  def main(args: Array[String]): Unit = {

    def buildTuple[T](name: String, dataType: String, defaultValue: T, description: String): (String, String, T, String) =
      (name, dataType, defaultValue, description)

    val local: (String, String, Boolean, String) = buildTuple("-local", "Boolean", false, "(T=local, F=cluster)")
    val debug: (String, String, Boolean, String) = buildTuple("-debug", "Boolean", false, "(T=show_debug, F=no_debug)")
    val driverMemory: (String, String, String, String) = buildTuple("-driverMemory", "String", "", "value set during local mode for spark.driver.memory")
    val executorMemory: (String, String, String, String) = buildTuple("-executorMemory", "String", "", "value set during local mode for spark.executor.memory")
    val numExecutors: (String, String, Int, String) = buildTuple("-numExecutors", "Int", 0, "value set during local mode for spark.num.executors")
    val executorCores: (String, String, Int, String) = buildTuple("-executorCores", "Int", 0, "value set during local mode for spark.executor.cores")
    val outDir: (String, String, Null, String) = buildTuple("-outDir", "String", null, "File location to write benchmark results")
    val firstSet: (String, String, Null, String) = buildTuple("-firstSet", "String", null, "First data set input file path (LION Streets)")
    val firstSetObjType: (String, String, Null, String) = buildTuple("-firstSetObjType", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")
    val secondSet: (String, String, Null, String) = buildTuple("-secondSet", "String", null, "Second data set input file path (Bus, TPEP, Yellow)")
    val secondSetObjType: (String, String, Null, String) = buildTuple("-secondSetObjType", "String", null, "The object type indicator (e.g. LION_LineString, TPEP_Point ...)")
    val k: (String, String, Int, String) = buildTuple("-k", "Int", 3, "Value of k")
    val indexType: (String, String, Null, String) = buildTuple("-indexType", "String", null, "qt for QuadTree, kdt for K-d Tree")
    val knnJoinType: (String, String, Null, String) = buildTuple("-knnJoinType", "String", null, "knn for Left knn Right, allKNN for both datasets")

    def lstArgInfo() = List(local, debug, driverMemory, executorMemory, numExecutors, executorCores, outDir, firstSet, firstSetObjType, secondSet, secondSetObjType, k, knnJoinType, indexType)

    val arrArgs = "-local F -debug T -firstSet1 /media/cusp/Data/GeoMatch_Files/InputFiles/RandomSamples_OLD/Taxi_1_A.csv -firstSetObjType Three_Part_Line -secondSet /media/cusp/Data/GeoMatch_Files/InputFiles/RandomSamples_OLD/Taxi_1_B.csv -secondSetObjType Three_Part_Line -k 10 -indexType qt -knnJoinType knn -outDir /media/cusp/Data/GeoMatch_Files/OutputFiles/sKNN/Taxi_1_Grp1_qt_knn"
      .split(" ")

    println(CLArgsParser(arrArgs, lstArgInfo()))
  }
}