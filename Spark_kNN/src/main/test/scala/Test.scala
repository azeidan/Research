import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.SizeEstimator
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.ds.geom.{Geom2D, Point, Rectangle}
import org.cusp.bdi.ds.kdt.KdTree
import org.cusp.bdi.sknn.SparkKnn
import org.cusp.bdi.util.LocalRunConsts

import scala.util.Random

object Test {

  def main(args: Array[String]): Unit = {

    val rect = Rectangle(new Geom2D(50000), new Geom2D(50000))

    val kdt = new KdTree
    kdt.insert(rect, (0 to 5).map(i => {

      val userData = new String(Array.fill(16)('@'))

      new Point(Random.nextInt(1000000), Random.nextInt(1000000), userData)
    }).iterator, 1)

    //    println(SizeEstimator.estimate(new Point(0, 0,"         ")))
    println(SizeEstimator.estimate(kdt))

    //    val sparkConf = new SparkConf()
    //      .setAppName(this.getClass.getName)
    //      .set("spark.serializer", classOf[KryoSerializer].getName)
    //      .registerKryoClasses(SparkKnn.getSparkKNNClasses)
    //      .set("spark.local.dir", LocalRunConsts.sparkWorkDir)
    //      .setMaster("local[*]")
    //
    //    val sc = new SparkContext(sparkConf)
    //
    //    val rdd1 = getRDD(sc, 1)
    //
    //    val rdd2 = getRDD(sc, 2)
    //
    //    //    val rddRes = rdd1.reduce().union(rdd2)
    //    //
    //    //    println("Done: " + rddRes)
  }

  private def getRDD(sc: SparkContext, code: Int) = {

    println("getRDD: ", code)

    sc.textFile(LocalRunConsts.pathRandSample_A_NAD83)
      .mapPartitions(_.map(line => {
        print(">")
        line
      }))
  }
}
