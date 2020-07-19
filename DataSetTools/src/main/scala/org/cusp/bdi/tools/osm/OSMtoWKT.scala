package org.cusp.bdi.tools.osm

import java.io.PrintWriter

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.xml.XML

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.cusp.bdi.util.Helper
import scala.io.Source
import java.io.File

abstract class BaseObj(_info: String) { def info = _info }
class NodeObj(_info: String) extends BaseObj(_info) {}
class WayObj(_info: String) extends BaseObj(_info) {}

object OSMtoWKT {

    def main(args: Array[String]): Unit = {

        val startTime = System.currentTimeMillis()

        if (args.length < 4)
            throw new Exception("Usage <tag Key value pair(k=v,k=v) e.g. natural=water or building=*> <Lon/Lat range \"fromLon,fromLat,toLon,toLat\"> <inputFilePath> <oututFilePath>")

        val mapTagKeyValue = args(0).split(",").map(kvPair => {

            val arr = kvPair.split("=")

            (arr(0), arr(1))
        })
            .toMap

        val rangeLonLat = args(1).split(",")
        val inputFilePath = new File(args(2))
        var outputFilePath = Helper.randOutputDir(args(3))

        val fromLon = rangeLonLat(0).toDouble
        val fromLat = rangeLonLat(1).toDouble
        val toLon = rangeLonLat(2).toDouble
        val toLat = rangeLonLat(3).toDouble

        new File(outputFilePath).mkdir()

        val mapNodes = Source.fromFile(inputFilePath).getLines.map(line => {

            var ret: (String, (String, String)) = null

            val line2 = line.toLowerCase()

            if (line2.indexOf("<node") != -1) {

                var idx = Helper.indexOf(line2, "lat") + 5
                val nodeLat = line2.substring(idx, line2.indexOf("\"", idx)).toDouble

                idx = Helper.indexOf(line2, "lon") + 5
                val nodeLon = line2.substring(idx, line2.indexOf("\"", idx)).toDouble

                if (fromLon <= nodeLon && nodeLon <= toLon && fromLat <= nodeLat && nodeLat <= toLat) {

                    idx = Helper.indexOf(line2, "id") + 4

                    val id = line2.substring(idx, line2.indexOf("\"", idx))

                    ret = (id, (nodeLon.toString(), nodeLat.toString()))
                }
            }

            ret
        })
            .filter(_ != null)
            .toMap

        val sb = StringBuilder.newBuilder
        var collect = false

        val pw = new PrintWriter(outputFilePath + File.separator + inputFilePath.getName + ".wkt")

        Source.fromFile(inputFilePath).getLines.foreach(line => {

            val line2 = line.toLowerCase()

            if (collect)
                sb.append(line)
            else if (line2.indexOf("<way") != -1) {

                collect = true
                sb.append(line)
            }

            if (sb.indexOf("way>") != -1) {

                val elm = XML.loadString(sb.toString())

                val sb2 = StringBuilder.newBuilder

                if ((elm \ "tag").filter(node => {

                    val keyValue = mapTagKeyValue.get(node.attribute("k").get.text)

                    keyValue != None && (keyValue.get.equals("*") || keyValue.get.equals(node.attribute("v").get.text))
                }).size > 0) {

                    val id = elm.attribute("id").get.text

                    (elm \ "nd").map(_.attribute("ref").get.text)
                        .foreach(ref => {

                            val point = mapNodes.get(ref)

                            if (point != None)
                                sb2.append("%s %s,".format(point.get._1, point.get._2))
                        })

                    if (sb2.size > 0)
                        sb2.insert(0, "%s,\"POLYGON((".format(id))
                            .append("))\"")
                }

                if (sb2.size > 0)
                    pw.println(sb2)

                sb.clear()
                collect = false
            }
        })
        pw.flush()
        pw.close()
        //        // node objects
        //        val rddNode = sparkContext.textFile(inputFilePath)
        //            .mapPartitions(_.map(line => {
        //
        //                var retVal: (String, BaseObj) = null
        //
        //                val line2 = line.toLowerCase()
        //
        //                if (line2.indexOf("<node") != -1) {
        //
        //                    val sb = StringBuilder.newBuilder
        //                        .append(line2)
        //
        //                    if (sb.indexOf("/>") == -1)
        //                        sb.insert(sb.length - 2, "/")
        //
        //                    val elm = XML.loadString(sb.toString())
        //
        //                    val nodeLon = elm.attribute("lon").get.head.text.toDouble
        //                    val nodeLat = elm.attribute("lat").get.head.text.toDouble
        //
        //                    if (fromLon <= nodeLon && nodeLon <= toLon && fromLat <= nodeLat && nodeLat <= toLat) {
        //
        //                        val bo: BaseObj = new NodeObj(elm.attribute("lon").get.head.text + " " + elm.attribute("lat").get.head.text)
        //
        //                        retVal = (elm.attribute("id").get.head.text, bo)
        //                    }
        //                }
        //
        //                retVal
        //            }))
        //            .filter(_ != null)
        //
        //        // Way objects
        //        val rddWay = sparkContext.textFile(inputFilePath)
        //            .mapPartitions(iter => {
        //
        //                val listWay = ListBuffer[(String, BaseObj)]()
        //                val sb = StringBuilder.newBuilder
        //                var collect = false
        //
        //                while (iter.hasNext) {
        //
        //                    val line = iter.next.toLowerCase()
        //
        //                    if (collect)
        //                        sb.append(line)
        //                    else if (line.indexOf("<way") != -1) {
        //
        //                        collect = true
        //                        sb.append(line)
        //                    }
        //
        //                    if (sb.indexOf("way>") != -1) {
        //
        //                        val elm = XML.loadString(sb.toString())
        //
        //                        if ((elm \ "tag").filter(node => {
        //                            val l = node.attribute("k").get.text
        //                            var result = false
        //                            val keyValue = tagKeyValue.get(node.attribute("k").get.text)
        //                            val b = keyValue != None && (keyValue.get.equals("*") || keyValue.get.equals(node.attribute("v").get.text))
        //                            keyValue != None && (keyValue.get.equals("*") || keyValue.get.equals(node.attribute("v").get.text))
        //                        }).size > 0) {
        //
        //                            val id = elm.attribute("id").get.text
        //
        //                            (elm \ "nd").map(_.attribute("ref").get.text).foreach(ref => listWay += ((ref, new WayObj(id))))
        //                        }
        //
        //                        sb.clear()
        //                        collect = false
        //                    }
        //                }
        //
        //                listWay.iterator
        //            })
        //
        //        val part = GMPartitioner(Math.max(rddNode.getNumPartitions, rddWay.getNumPartitions))
        //
        //        rddNode.partitionBy(part)
        //            .union(rddWay.partitionBy(part))
        //            .mapPartitions(iter => {
        //
        //                val mapNodes = HashMap[String, String]()
        //
        //                iter.map(row => {
        //
        //                    row._2 match {
        //                        case nodeObj: NodeObj => {
        //                            mapNodes += row._1 -> nodeObj.info
        //                            null
        //                        }
        //                        case wayObj: WayObj => {
        //
        //                            val coords = mapNodes.get(row._1)
        //
        //                            if (coords == None)
        //                                null
        //                            else
        //                                (wayObj.info, coords.get)
        //                        }
        //                        case _ => { throw new Exception("Unkown type") }
        //                    }
        //                })
        //            })
        //            .filter(_ != null)
        //            .reduceByKey((coord1, coord2) => coord1 + "," + coord2)
        //            .mapPartitions(_.map(x => x._1 + ",\"POLYGON((" + x._2 + "))\""))
        //            .saveAsTextFile(outputFilePath, classOf[GzipCodec])

        println("Output file: " + outputFilePath)
        println("Total Runtime: %,d".format(System.currentTimeMillis() - startTime))
    }
}