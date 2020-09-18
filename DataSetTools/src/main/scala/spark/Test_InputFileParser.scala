package spark

import org.apache.spark.{SparkConf, SparkContext}

object Test_InputFileParser {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    def busPoints = (line: String) => {

      val xy = getXY(line, 1, true)

      if (xy == null)
        null
      else
        (line, xy)
    }

    def getXY(line: String, startCommaNum: Int, removeDecimal: Boolean) = {

      def getCommaPos(commaNum: Int, startIdx: Int) = {

        var count = 0
        var idx = startIdx

        while (count < commaNum) {

          if (line(idx) == ',')
            count += 1

          idx += 1
        }

        idx
      }

      try {

        val idx0 = getCommaPos(startCommaNum, 0)
        val idx1 = getCommaPos(1, idx0 + 1)
        val idx2 = getCommaPos(1, idx1 + 1)

        var x = line.substring(idx0, idx1 - 1)
        var y = line.substring(idx1, idx2 - 1)

        if (removeDecimal) {

          x = x.substring(0, x.indexOf('.'))
          y = y.substring(0, y.indexOf('.'))
        }

        if (x(0) != '-' && (x(0) < '0' || x(0) > '9'))
          null
        else
          (x, y)
      }
      catch {
        case _: Exception => null
      }
    }

    // /gws/projects/project-taxi_capstone_2016/share/Bus_TripRecod_NAD83/part*.gz
    val count = sc.textFile("/gws/projects/project-taxi_capstone_2016/share/Bus_TripRecod_NAD83/part*")
      .mapPartitions(iter => Array(iter.map(busPoints).filter(_ == null).map(_ => 1L).sum).iterator)
      .fold(0)(_ + _)
    println("Count: %d", count)

    //            .mapPartitions(iter => {
    //
    //                val res = iter.fold((Double.MaxValue, Double.MinValue, 0.0, 0L))(foldOp)
    //
    //                Array(res).iterator
    //            })
    //            .fold((Double.MaxValue, Double.MinValue, 0.0, 0L))(foldOp)

    //        println("Min Distance: %,.7f%nMax Distance: %,.7f%nSum: %,.20f%nCount: %,d%nAvg: %,.20f%n".format(sum._1, sum._2, sum._3, sum._4, sum._3 / sum._4))
  }
}
