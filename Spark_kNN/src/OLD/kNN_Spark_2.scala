//import org.cusp.bdi.gm.GeoMatch
//import org.apache.spark.SparkConf
//import org.apache.spark.serializer.KryoSerializer
//import org.apache.spark.SparkContext
//import org.cusp.bdi.util.Helper
//import org.cusp.bdi.util.HilbertIndex
//import org.glassfish.hk2.api.Self
//import org.cusp.bdi.util.InputFileParsers
//import org.cusp.bdi.gm.geom.GMPoint
//import org.cusp.bdi.util.CustomPartitioner
//import org.apache.hadoop.io.compress.GzipCodec
//import org.locationtech.jts.index.kdtree.KdTree
//import org.locationtech.jts.geom.Coordinate
//import org.locationtech.jts.geom.Envelope
//import org.locationtech.jts.index.kdtree.KdNode
//import scala.collection.mutable.ListBuffer
//import java.util.List
//import org.locationtech.jts.geom.GeometryFactory
//import org.cusp.bdi.gm.geom.GMGeomBase
//import scala.collection.mutable.SortedSet
//
//object kNN_Spark_2 {
//
//    def main(args: Array[String]): Unit = {
//
//        val inputSet1 = "/media/cusp/Data/GeoMatch_Files/InputFiles/Bus_TripRecod_NAD83_part-00000_SMALL.csv"
//        val inputSet2 = inputSet1
//
//        val kParam = 3
//        val searchEnvExpandBy = 100
//
//        val minX = 0 // 909126
//        val minY = 0 //110626
//        val maxX = Int.MaxValue //1610215
//        val maxY = Int.MaxValue //424498
//
//        val gridWidth = maxX - minX
//        val gridHeight = maxY - minY
//
//        val hilbertSize = Math.pow(2, 25).toInt
//        val hilbertBoxWidth = gridWidth / hilbertSize
//        val hilbertBoxHeight = gridHeight / hilbertSize
//
//        val outDir = Helper.randOutputDir("/media/ayman/Data/GeoMatch_Files/OutputFiles/")
//
//        val sparkConf = new SparkConf()
//            .setAppName("kNN_Test")
//            //            .set("spark.hadoop.mapreduce.input.fileinputformat.split.minsize", (2*1024*1024).toString)
//            .set("spark.serializer", classOf[KryoSerializer].getName)
//            .registerKryoClasses(GeoMatch.getGeoMatchClasses())
//
//        sparkConf.setMaster("local[*]")
//        val sc = new SparkContext(sparkConf)
//
//        val startTime = System.currentTimeMillis()
//
//        val rddPlain1 = sc.textFile(inputSet1, 17)
//        val rddPlain2 = sc.textFile(inputSet2, 17)
//
//        val numPartitions = if (rddPlain1.getNumPartitions > rddPlain2.getNumPartitions) rddPlain1.getNumPartitions else rddPlain2.getNumPartitions
//
//        val customPartitioner = new CustomPartitioner(numPartitions)
//
//        val rdd1 = rddPlain1
//            .mapPartitions(_.map(InputFileParsers.busPoints).filter(_ != null))
//            .mapPartitions(_.map(row => {
//
//                val pointCoords = (row._2._1.toInt, row._2._2.toInt)
//
//                val gridCoords1 = (math.floor(pointCoords._1 / hilbertBoxWidth).toInt, math.floor(pointCoords._2 / hilbertBoxHeight).toInt)
//                val gridCoords2 = (math.floor(pointCoords._2 / hilbertBoxHeight).toInt, math.floor(pointCoords._1 / hilbertBoxWidth).toInt)
//
//                val hIdx1 = HilbertIndex.computeIndex(hilbertSize, gridCoords1)
//                val hIdx2 = HilbertIndex.computeIndex(hilbertSize, gridCoords2)
//
//                val part1 = hIdx1 % numPartitions
//                val part2 = hIdx2 % numPartitions
//
//                val point = new GMPoint(row._1, pointCoords)
//
//                val lst = ListBuffer((part1, point))
//
//                if (part2 != part1)
//                    lst.append((part2, point))
//
//                lst.iterator
//            }))
//            .flatMap(_.seq)
//            .partitionBy(customPartitioner)
//
//        val rdd2 = rddPlain2
//            .mapPartitions(_.map(InputFileParsers.busPoints).filter(_ != null))
//            .mapPartitions(_.map(row => {
//
//                val pointCoords = (row._2._1.toInt, row._2._2.toInt)
//
//                val gridCoords1 = (math.floor(pointCoords._1 / hilbertBoxWidth).toInt, math.floor(pointCoords._2 / hilbertBoxHeight).toInt)
//                val gridCoords2 = (math.floor(pointCoords._2 / hilbertBoxHeight).toInt, math.floor(pointCoords._1 / hilbertBoxWidth).toInt)
//
//                val hIdx1 = HilbertIndex.computeIndex(hilbertSize, gridCoords1)
//                val hIdx2 = HilbertIndex.computeIndex(hilbertSize, gridCoords2)
//
//                val part1 = hIdx1 % numPartitions
//                val part2 = hIdx2 % numPartitions
//
//                val point = new GMPoint(row._1, pointCoords)
//
//                val lst = ListBuffer((part1, point))
//
//                if (part2 != part1)
//                    lst.append((part2, point))
//
//                lst.iterator
//            }))
//            .flatMap(_.seq)
//            .partitionBy(customPartitioner)
//
//        rdd1.union(rdd2)
//            .mapPartitions(iter => {
//
//                val jtsGeomFact = new GeometryFactory
//                val lst = iter.toList
//                val kt = new KdTree()
//
//                var ktRootNode: KdNode = null
//
//                lst.foreach(pt => ktRootNode = kt.insert(new Coordinate(pt._2._pointCoord._1, pt._2._pointCoord._2), pt._2))
//
//                lst.map(pt => {
//
//                    var searchEnv = new Envelope(new Coordinate(pt._2._pointCoord._1, pt._2._pointCoord._2))
//
//                    var matches: List[_] = kt.query(searchEnv)
//
//                    while (matches.size() < kParam && matches.size() < ktRootNode.getCount) {
//
//                        searchEnv.expandBy(searchEnvExpandBy)
//                        matches = kt.query(searchEnv)
//                    }
//
//                    val orderedMatches = SortedSet[(Double, GMGeomBase)]()
//
//                    import scala.collection.JavaConversions._
//                    matches.foreach(_ match {
//                        case kdn: KdNode => {
//
//                            val matchPoint: GMGeomBase = kdn.getData match { case gmPoint: GMPoint => gmPoint }
//
//                            if (!pt._2.equals(matchPoint))
//                                orderedMatches.add((pt._2.toJTS(jtsGeomFact).get(0).distance(matchPoint.toJTS(jtsGeomFact).get(0)), matchPoint))
//                        }
//                    })
//
//                    (pt._2, orderedMatches.take(kParam))
//                })
//                    .iterator
//            })
//            .saveAsTextFile(outDir, classOf[GzipCodec])
//
//        printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
//
//        println(outDir)
//    }
//}