/**
  * @author
  */
package org.cusp.bdi.tools

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.io.compress.GzipCodec
import org.cusp.bdi.util.Helper

object RecordFind {
    def main(args: Array[String]): Unit = {

        if (args.length != 5)
            println("Usage: <Sequential input file> <GM utput dir> <output dir> <run locally> <search string>")
        else
            runSearch(args(0), args(1), args(2), Helper.isBooleanStr(args(3)), args(4).toLowerCase().split("<<>>"))
    }

    def runSearch(seqInput: String, gmInput: String, output: String, runLocal: Boolean, searchStr: Array[String]) {

        val startTime = System.currentTimeMillis()

        val appName = this.getClass.getName
        var outputDir = output
        val conf = new SparkConf().setAppName(appName)

        if (runLocal) {

            conf.setMaster("local[*]")

            // randomize output dir
            outputDir = Helper.randOutputDir(outputDir)
        }

        val sc = new SparkContext(conf)

        def matchLine(line: String) = {

            var i = 0
            var found = false
            while (!found && i < searchStr.length) {

                found = line.toLowerCase().startsWith(searchStr(i))
                i += 1
            }

            found
        }

        val rddFile1 = sc.textFile(gmInput)
            .filter(matchLine)
            .mapPartitions(_.map(line => "File1: " + line))

        val rddFile2 = sc.textFile(seqInput)
            .filter(matchLine)
            .mapPartitions(_.map(line => "File2: " + line))

        sc.parallelize(rddFile1.union(rddFile2).collect(), 1)
            .saveAsTextFile(outputDir, classOf[GzipCodec])

        if (runLocal) {

            println("Total Time: " + (System.currentTimeMillis() - startTime))
            println("Output idr: " + outputDir)
        }
    }
}