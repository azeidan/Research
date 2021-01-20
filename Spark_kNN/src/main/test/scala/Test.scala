import scala.collection.mutable
import scala.io.Source

object Test {

  def main(args: Array[String]): Unit = {
    //    val debugMode = true
    //    val lstDebugInfo = null
    //    val stackRangeInfo = mutable.Stack[RangeInfo]()
    //    val partObjCapacity = 739966
    //
    //    val lines = Source.fromFile("/home/cusp/Dropbox/Research/Projects/IntelliJ/Research/Spark_kNN/src/main/test/scala/TBD.CSV")
    //      .getLines()
    //      .map(line => {
    //        val arr = line.split(",")
    //        ((arr(0).toDouble, arr(1).toDouble), arr(2).toLong)
    //      })
    //      .foreach(row => { // group cells on partitions
    //
    ////        Helper.loggerSLf4J(debugMode, SparkKnn, ">>>\t%,.2f\t%,.2f\t%,d".format(row._1._1, row._1._2, row._2), lstDebugInfo)
    //
    //        if (stackRangeInfo.isEmpty || stackRangeInfo.top.totalWeight + row._2 > partObjCapacity)
    //          stackRangeInfo.push(new RangeInfo(row))
    //        else {
    //
    //          stackRangeInfo.top.lstMBRCoord += row
    //          stackRangeInfo.top.totalWeight += row._2
    //          stackRangeInfo.top.right = row._1._1
    //
    //          if (row._1._2 < stackRangeInfo.top.bottom)
    //            stackRangeInfo.top.bottom = row._1._2
    //          else if (row._1._2 > stackRangeInfo.top.top)
    //            stackRangeInfo.top.top = row._1._2
    //        }
    //      })
    //
    //    var partCounter = -1
    //    stackRangeInfo.reverse.foreach(rangeInfo => {
    //      partCounter+=1
    //      println(">>%s\t%,d".format(rangeInfo.toString, partCounter))
    //    })
  }
}
