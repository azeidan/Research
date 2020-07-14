package org.cusp.bdi.sknn.util

import scala.collection.SortedSet

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer
import org.cusp.bdi.gm.GeoMatch
import org.cusp.bdi.util.LocalRunFilePaths

object FileCompare {

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf()
            .setAppName("kNN_Test")
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .registerKryoClasses(GeoMatch.getGeoMatchClasses())

        sparkConf.setMaster("local[*]")
        val sc = new SparkContext(sparkConf)

        val startTime = System.currentTimeMillis()

        val rddKey = sc.textFile(LocalRunFilePaths.pathKM_Bus_SMALL).mapPartitions(_.map(parseLine))
            .reduceByKey(_ ++ _)
            .mapPartitions(_.map(x => {

                val sSet = x._2.groupBy(_._2)
                    .mapValues(x => x.head)
                    .toArray
                    .map(_._2)
                    .take(10)
                    .map(_._2)

                (x._1, sSet)
            }))

        val rddKNN = sc.textFile(LocalRunFilePaths.pathKNN_FW_FileCompare).mapPartitions(_.map(parseLine))
            .reduceByKey(_ ++ _)
            .mapPartitions(_.map(x => {
                //if(x._1.startsWith("2014-09-08 00:41:46,1004329.0732849675,208988.5200546987,q39,3577,q390076,1,551434,0,36.42,8573.2"))
                //    println()
                val sSet = x._2.groupBy(_._2).map(x => x)
                    .mapValues(x => x.head)
                    .toArray
                    .map(_._2)
                    .take(3)
                    .map(_._2)

                (x._1, sSet)
            }))

        val hashPartitioner = new HashPartitioner(Math.max(rddKey.getNumPartitions, rddKNN.getNumPartitions))

        rddKey.partitionBy(hashPartitioner)
            .union(rddKNN.partitionBy(hashPartitioner))
            .reduceByKey((arrKey, arrKNN) => {

                // ("# OK", "# over match", "# under match")
                if (arrKey.length > 0 && arrKNN.length == 0)
                    Array("0", "0", "1")
                else if (arrKey.length == 0 && arrKNN.length > 0)
                    Array("0", "1", "0")
                else if (arrKey.length == 0 && arrKNN.length == 0)
                    Array("1", "0", "0")
                else if (arrKey.length < arrKNN.length)
                    Array("0", "1", "0")
                else if (arrKey.length > arrKNN.length && arrKNN.length < 3)
                    Array("0", "0", "1")
                else {

                    val matchCount = arrKey.map(lineKey => {

                        val idx0 = lineKey.indexOf(',')
                        // val distance0 = lineKey.substring(0, idx0)
                        val tripRecord0 = lineKey.substring(idx0 + 1)

                        arrKNN.map(lineKNN => {

                            val idx1 = lineKey.indexOf(',')
                            // val distance1 = lineKey.substring(0, idx1)
                            val tripRecord1 = lineKey.substring(idx1 + 1)

                            if (tripRecord0.equalsIgnoreCase(tripRecord1))
                                1
                            else
                                0
                        })
                    }).flatMap(_.seq)
                        .sum

                    if (matchCount == 0)
                        Array("0", "0", "1")
                    else
                        Array("1", "0", "0")
                }
            })
            .mapPartitions(_.map(row => {

                try {
                    row._2.map(_.toInt)
                }
                catch {
                    case ex: Exception =>
                        println()
                }

                row._2.map(_.toInt)
            }))
            .fold(Array(0, 0, 0))((arr0, arr1) => Array(arr0(0) + arr1(0), arr0(1) + arr1(1), arr0(2) + arr1(2)))
            .foreach(println)

        printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
    }

    private def parseLine(line: String) = {

        val arrLine = line.split('[')

        val arrMatches = arrLine(1).split(')')
            .map(part =>
                if (part.charAt(0) == '(')
                    part.substring(1)
                else if (part.charAt(0) == ',') {
                    if (part.charAt(1) == '(')
                        part.substring(2)
                    else if (part.charAt(1) == ']')
                        null
                    else
                        part
                }
                else if (part.charAt(0) == ']')
                    null
                else
                    part)

            .filter(_ != null)
            .map(line => {

                val idx = line.indexOf(',')

                (line.substring(0, idx).toDouble, line.substring(idx + 1))
            })
            .to[SortedSet]

        (arrLine(0).toLowerCase(), arrMatches)
    }
}

//case class CustomPartitioner(numParts: Int) extends Partitioner {
//
//    override def numPartitions = numParts
//
//    override def getPartition(key: Any): Int = {
//
//        key match {}
//    }
//}