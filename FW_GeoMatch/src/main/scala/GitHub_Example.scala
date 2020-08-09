import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.gm.GeoMatch
import org.cusp.bdi.gm.geom.{GMLineString, GMPoint}

object GitHub_Example {
  def main(args: Array[String]): Unit = {

    // spark initialization including Kryo
    val sparkConf = new SparkConf()
      .setAppName("GeoMatch_Test")
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .registerKryoClasses(GeoMatch.getGeoMatchClasses())

    // assuming local run
    sparkConf.setMaster("local[*]")

    val sparkContext = new SparkContext(sparkConf)

    // The first dataset.
    val rddFirstSet = sparkContext.textFile("firstDataset.csv")
      .mapPartitions(_.map(line => {

        // parse the line and form the spatial object
        val parts = line.split(',')

        val arrCoords = parts.slice(1, parts.length)
          .map(xyStr => {
            val xy = xyStr.split(' ')

            (xy(0).toDouble, xy(1).toDouble)
          })

        // create the spatial object GMLineString. The first parameter is the payload
        // the second parameter is the list of coordinates that form the LineString
        new GMLineString(parts(0), arrCoords)
      }))

    // The second dataset.
    val rddSecondSet = sparkContext.textFile("secondDataset.csv")
      .mapPartitions(_.map(line => {

        // parse the line and form the spatial object
        val parts = line.split(',')

        // create the spatial object GMPoint. The first parameter is the payload
        // the second parameter is the point's coordinates
        new GMPoint(parts(0), (parts(1).toDouble.toInt, parts(2).toDouble.toInt))
      }))

    // initialize GeoMatch
    val geoMatch = new GeoMatch(false, 256, 150, (-1, -1, -1, -1))

    // perform map-matching
    val resultRDD = geoMatch.spatialJoinKNN(rddFirstSet, rddSecondSet, 3, false)

    // print the results
    resultRDD.mapPartitions(_.map(row => println("%-10s%s".format(row._1.payload, row._2.map(_.payload).mkString(",")))))
      .collect()
  }
}