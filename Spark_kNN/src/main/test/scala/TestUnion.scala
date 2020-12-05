import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.cusp.bdi.sknn.SparkKnn
import org.cusp.bdi.util.LocalRunConsts

object TestUnion {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getName)
      .set("spark.serializer", classOf[KryoSerializer].getName)
      .registerKryoClasses(SparkKnn.getSparkKNNClasses)
      .set("spark.local.dir", LocalRunConsts.sparkWorkDir)
      .setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd1 = getRDD(sc, 1)

    val rdd2 = getRDD(sc, 2)

    //    val rddRes = rdd1.reduce().union(rdd2)
    //
    //    println("Done: " + rddRes)
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
