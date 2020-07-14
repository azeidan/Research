package org.cusp.bdi.sknn

import scala.collection.mutable.ListBuffer

import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.DistanceMeasure
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.gm.GeoMatch
import org.cusp.bdi.gm.geom.GMGeomBase
import org.cusp.bdi.gm.geom.GMPoint
import org.cusp.bdi.sknn.util.RDD_Store
import org.cusp.bdi.sknn.util.SparkKNN_Arguments
import org.cusp.bdi.util.Helper
import org.cusp.bdi.util.sknn.SparkKNN_Local_CLArgs
import org.locationtech.jts.index.strtree.AbstractNode
import org.locationtech.jts.index.strtree.ItemBoundable

import com.insightfullogic.quad_trees.Box
import com.insightfullogic.quad_trees.Point
import com.insightfullogic.quad_trees.QuadTree
import com.insightfullogic.quad_trees.QuadTreeAddendum

//case class Region() extends Serializable /*with Ordered[MBR]*/ {
//
//    var uniqueID: String = null
//    var pointCount = 0L
//    var assignedPart = "-1"
//    var minX = Double.MaxValue
//    var minY = Double.MaxValue
//    var maxX = Double.MinValue
//    var maxY = Double.MinValue
//
//    def contains(pointXY: (Double, Double)) =
//        pointXY._1 >= minX && pointXY._1 <= maxX && pointXY._2 >= minY && pointXY._2 <= maxY
//
//    def diffFromCenter(pointXY: (Double, Double)) =
//        (math.abs(center._1 - pointXY._1), math.abs(center._2 - pointXY._2))
//
//    def add(point: Point) {
//
//        if (point.x < minX) minX = point.x
//        if (point.y < minY) minY = point.y
//        if (point.x > maxX) maxX = point.x
//        if (point.y > maxY) maxY = point.y
//
//        //        pointCount += 1
//    }
//
//    def width = maxX - minX
//
//    def height = maxY - minY
//
//    def center =
//        (minX + (width / 2), minY + (height / 2))
//
//    def get() =
//        ((minX, minY, maxX, maxY))
//
//    override def toString() =
//        "%s\t%s\t%.8f\t%.8f\t%.8f\t%.8f\t%d".format(assignedPart, uniqueID, minX, minY, maxX, maxY, pointCount)
//
//    def expandBy(expandBy: Double) = {
//        minX -= expandBy
//        minY -= expandBy
//        maxX += expandBy
//        maxY += expandBy
//    }
//}

object SparkKNN {

    def main(args: Array[String]): Unit = {

        val startTime = System.currentTimeMillis()
        var startTime2 = startTime
        //        val hilbertN = math.sqrt(Long.MaxValue).toLong

        val clArgs = SparkKNN_Local_CLArgs.randomPoints_randomPoints(SparkKNN_Arguments())
        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift(SparkKNN_Arguments())
        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint(SparkKNN_Arguments())
        //        val clArgs = SparkKNN_Local_CLArgs.tpepPoint_tpepPoint(SparkKNN_Arguments())
        //        val clArgs = CLArgsParser(args, SparkKNN_Arguments())

        val sparkConf = new SparkConf()
            .setAppName(this.getClass.getName)
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .registerKryoClasses(GeoMatch.getGeoMatchClasses())
            .registerKryoClasses(Array(classOf[KeyBase],
                                       classOf[Key0],
                                       classOf[Key1],
                                       classOf[QuadTree],
                                       classOf[QuadTreeAddendum]))

        if (clArgs.getParamValueBoolean(SparkKNN_Arguments.local))
            sparkConf.setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val kParam = clArgs.getParamValueInt(SparkKNN_Arguments.k)
        val kmMultiplier = clArgs.getParamValueDouble(SparkKNN_Arguments.kmMultiplier)
        val minPartitions = clArgs.getParamValueInt(SparkKNN_Arguments.minPartitions)
        val outDir = clArgs.getParamValueString(SparkKNN_Arguments.outDir)

        // delete output dir if exists
        Helper.delDirHDFS(sc, clArgs.getParamValueString(SparkKNN_Arguments.outDir))

        val rddDS1Plain = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)

        //        val execCores = sparkConf.getInt("spark.executor.cores", 1)

        // 7% reduction in memory to account for overhead operations
        val execMem = 0.93 * Helper.toByte(sparkConf.get("spark.executor.memory", sc.getExecutorMemoryStatus.map(_._2._1).max + "B")) // skips memory of core assigned for Hadoop daemon
        // deduct yarn overhead
        // execMem -= math.ceil(math.max(384, .07 * execMem)).toLong

        val (maxRowSize, rowCount) = sc.runJob(rddDS1Plain, getIteratorSize _, Array(0, rddDS1Plain.getNumPartitions / 2)).head

        val totalRowCount = rowCount * rddDS1Plain.getNumPartitions

        val gmGeomDummy = GMPoint((0 until maxRowSize).map(i => " ").mkString(""), (0, 0))
        val pointDummy = Point(0, 0)
        val qtDummy = new QuadTree(Box(Point(0, 0), Point(0, 0)))
        val sSetDummy = SortSetObj(kParam)
        (0 until kParam).foreach(i => sSetDummy.add((i, gmGeomDummy)))
        val geomSetDummy = (gmGeomDummy, sSetDummy)

        val pointCost = SizeEstimator.estimate(pointDummy)
        val geomSetCost = SizeEstimator.estimate(geomSetDummy)
        val qTreeCost = SizeEstimator.estimate(qtDummy)
        // *2 to account for qTree operational overhead of Points
        var adjustedRowsPerExec = ((execMem - (2 * qTreeCost)) / (pointCost + geomSetCost)).toLong

        var numParts = math.ceil(totalRowCount.toDouble / adjustedRowsPerExec).toInt

        if (numParts == 1) {

            numParts = rddDS1Plain.getNumPartitions
            adjustedRowsPerExec = totalRowCount / numParts
        }

        //        println("<<execMem=" + execMem)
        //        println("<<maxRowSize=" + maxRowSize)
        //        println("<<rowCount=" + rowCount)
        //        println("<<totalRowCount=" + totalRowCount)
        //        println("<<geomSetCost=" + geomSetCost)
        //        println("<<qTreeCost=" + qTreeCost)
        //        println("<<adjustedRowsPerExec=" + adjustedRowsPerExec)
        //        println("<<numParts=" + numParts)

        var rddDS1_XY = rddDS1Plain
            .mapPartitionsWithIndex((pIdx, iter) => {

                if (pIdx % 3 == 0)
                    iter
                else
                    Iterator[String]()
            })
            .filter(_.size > 0)
            .mapPartitions(_.map(RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj))))
            .mapPartitions(_.map(row => ((row._2._1.toDouble, row._2._2.toDouble), null)))
            .reduceByKey((x, y) => x)
            .mapPartitions(_.map(row => Vectors.dense(row._1._1, row._1._2)))
            .persist()

        //        println("<<Number of partitions: %d%n<<kmMultiplier: %f%n<<numParts: %d".format(inputNumPart, kmMultiplier, numParts))

        var kmeans = new KMeans().setK(numParts).setSeed(1L).setDistanceMeasure(DistanceMeasure.EUCLIDEAN)
        var kmModel = kmeans.run(rddDS1_XY)

        rddDS1_XY.unpersist(false)
        rddDS1_XY = null

        println(">T>Kmeans: %f".format((System.currentTimeMillis() - startTime2) / 1000.0))

        //        kmModel.clusterCenters.foreach(x => println(">>%s".format(x)))

        startTime2 = System.currentTimeMillis()

        val partitionerByK = new Partitioner() {

            override def numPartitions = numParts
            override def getPartition(key: Any): Int =
                key match {
                    case keyBase: KeyBase => keyBase.partId.toInt
                    case key: Int => key
                }
        }

        // index
        val arrQTSummary = rddDS1Plain
            //            .mapPartitions(_.map(RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj))))
            .mapPartitions(iter => {

                val lineParser = RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj))

                iter.map(line => {

                    val row = lineParser(line)

                    val kMeansK = kmModel.predict(Vectors.dense(row._2._1.toDouble, row._2._2.toDouble))

                    (kMeansK, row)
                })
            })
            .partitionBy(partitionerByK)
            .mapPartitions(iter => {

                val lstPoints = ListBuffer[Point]()
                var minX = Double.PositiveInfinity
                var minY = Double.PositiveInfinity
                var maxX = Double.NegativeInfinity
                var maxY = Double.NegativeInfinity
                var partK = -1

                iter.foreach(row => {

                    partK = row._1

                    val (payload, xyStr) = row._2

                    val point = Point(xyStr._1.toDouble, xyStr._2.toDouble)

                    lstPoints.append(point)

                    if (point.x < minX) minX = point.x
                    if (point.y < minY) minY = point.y
                    if (point.x > maxX) maxX = point.x
                    if (point.y > maxY) maxY = point.y
                })

                // build QuadTree
                val widthHalf = (maxX - minX) / 2
                val heightHalf = (maxY - minY) / 2
                val qTree = new QuadTree(Box(Point(widthHalf + minX, heightHalf + minY), Point(widthHalf, heightHalf)))

                lstPoints.foreach(point =>
                    if (!qTree.insert(point)) {
                        // qTree.insert(point)
                        throw new Exception("Insert failed " + point)
                    })

                if (partK == 0)
                    println

                qTree.computeSubRegions(100) //adjustedRowsPerExec)

                // estimate search region

                //                println(qtPoint)

                //                    qt.printAllBoundaries()

                //                var prevTotalPoints = 0L
                //
                //                var prevQT: QuadTree = null
                //
                //                while (qtPoint != null) {
                //                    while (qtPoint.parent != null && (qtPoint.totalPoints - prevTotalPoints) <= adjustedRowsPerExec) {
                //
                //                        println(qt.totalPoints)
                //
                //                        val tp = qt.totalPoints
                //
                //                        qt = qt.parent
                //                    }
                //                    prevTotalPoints += adjustedRowsPerExec
                //                    lst.append(qt.getSummaryQT(prevQT))
                //
                //                    prevQT = qt
                //                    qt = qt.parent
                //                }
                //
                //                //                Iterator((partK, qTree))

                //                lst.map(qts => (partK, qts)).iterator
                qTree.abridge()
                Iterator((partK, qTree))
            }, true)
            .collect()

        arrQTSummary.foreach(summary =>
            println(">>%d,%s%n%s".format(summary._1, summary._2.boundary.mbr, summary._2.arrSegments.mkString("\t"))))
        //
        println(SizeEstimator.estimate(arrQTSummary))

        arrQTSummary.foreach(summary =>
            println(">>%s".format(SizeEstimator.estimate(summary._2))))

        //        val rddDS1 = rddDS1Plain
        //            .mapPartitions(_.map(RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj))))
        //            .mapPartitions(_.map(row => {
        //
        //                val coordXY = (row._2._1.toDouble, row._2._2.toDouble)
        //
        //                // val kMeansK = kmModel.predict(Vectors.dense(x, y))
        //
        //                val partNum = getBestPartition(arrQTSummary, coordXY)
        //
        //                if (partNum == -1)
        //                    throw new Exception("Unhandeled state: point from DS1 isn't contained in any of the regions")
        //
        //                (partNum, row)
        //            }))
        //            .partitionBy(partitionerByK)
        //            .mapPartitions(iter => {
        //
        //                val lstPoints = ListBuffer[Point]()
        //                var minX = Double.MaxValue
        //                var minY = Double.MaxValue
        //                var maxX = Double.MinValue
        //                var maxY = Double.MinValue
        //                var partK = -1
        //
        //                iter.foreach(row => {
        //
        //                    partK = row._1
        //
        //                    val (payload, xyStr) = row._2
        //
        //                    val gmGeomBase: GMGeomBase = new GMPoint(payload, (xyStr._1.toDouble, xyStr._2.toDouble))
        //
        //                    val point = Point(gmGeomBase.coordArr(0)._1.toDouble, gmGeomBase.coordArr(0)._2.toDouble, (gmGeomBase, SortSetObj(kParam)))
        //
        //                    lstPoints.append(point)
        //
        //                    if (point.x < minX) minX = point.x
        //                    if (point.y < minY) minY = point.y
        //                    if (point.x > maxX) maxX = point.x
        //                    if (point.y > maxY) maxY = point.y
        //                })
        //
        //                // build QuadTree
        //                val widthHalf = (maxX - minX) / 2
        //                val heightHalf = (maxY - minY) / 2
        //                val qTree = new QuadTree(Box(Point(widthHalf + minX, heightHalf + minY), Point(widthHalf + 0.1, heightHalf + 0.1)))
        //
        //                lstPoints.foreach(qTree.insert(_))
        //
        //                //                if (partK == 5)
        //                //                    print()
        //
        //                val key0: KeyBase = Key0(partK)
        //                val ret: (KeyBase, Any) = (key0, qTree)
        //
        //                //                println(">>\t%s\t%f\t%f\t%f\t%f\t%d".format(key0.partId, qTree.boundary.left, qTree.boundary.bottom, qTree.boundary.right, qTree.boundary.top, qTree.getTotalPoints))
        //                //                println(">>>\t%s\t%s\t%s\t%s\t%s".format(key0.partId, tmpMinX, tmpMinY, tmpMaxX, tmpMaxY))
        //
        //                Iterator(ret)
        //            }, true)
        //        //                        .partitionBy(partitionerByK)
        //
        //        val rddDS2 = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), minPartitions)
        //            .mapPartitionsWithIndex((pIdx, iter) => {
        //
        //                val lineParser = RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj))
        //
        //                iter.map(line => {
        //
        //                    val row = lineParser(line)
        //
        //                    val coordXY = (row._2._1.toDouble, row._2._2.toDouble)
        //
        //                    val gmGeom: GMGeomBase = new GMPoint(row._1, coordXY)
        //
        //                    // Compute best partition
        //                    val point = Point(coordXY._1, coordXY._2)
        //
        //                    //                    val qTree = qTreeGlobal.queryPoint(point)
        //                    //
        //                    //                    val partNum = qTree.getPoints.head.getUserData.toString
        //
        //                    var partNum = getBestPartition(arrQTSummary, coordXY)
        //
        //                    if (partNum == -1)
        //                        partNum = arrQTSummary.map(summary => {
        //
        //                            val dist = Helper.squaredDist((coordXY._1, coordXY._2), (summary._2.boundary.center.x, summary._2.boundary.center.y))
        //
        //                            (dist, summary)
        //                        })
        //                            .minBy(_._1)
        //                            ._2._1
        //
        //                    //
        //                    //                    var qTreeSummaryQuad: QuadTreeSummary = null
        //                    //                    var searchRegion: Circle = null
        //                    //
        //                    //                    if (partNum == -1) {
        //                    //
        //                    //                    }
        //                    //                    else {
        //                    //
        //                    //                        qTreeSummaryQuad = QTreeOperations.getBestLeaf(arrQTSummary.find(_._1 == partNum).get._2, point)
        //                    //                    }
        //                    //
        //                    //                    do {
        //                    //
        //                    //                        //            if (gmGeom.payload.equalsIgnoreCase("rb_126910"))
        //                    //                        //                println()
        //                    //
        //                    //                        searchRegion = new Circle(Point(point.x, point.y), math.max(qTreeSummaryQuad.boundary.halfDimension.x, qTreeSummaryQuad.boundary.halfDimension.y))
        //                    //
        //                    //                        qTreeSummaryQuad = qTreeSummaryQuad.parent
        //                    //
        //                    //                    } while (qTree != null && ssGmGeom.size < k)
        //
        //                    val key: KeyBase = Key1(partNum)
        //
        //                    val ret: (KeyBase, Any) =
        //                        (key, (gmGeom, SortSetObj(kParam)))
        //
        //                    //                    println(">>%s\t%s\t%s".format(ret._1, ret._2._2.payload, ret._2._4.mkString("\t")))
        //
        //                    ret
        //                })
        //            })
        //            .partitionBy(partitionerByK)
        //
        //        var rdd = rddDS1.union(rddDS2)
        //
        //        //        println(rdd.toDebugString)
        //
        //        //        //        (0 until 8).foreach(i => {
        //        //
        //        //        //            if (i > 0)
        //        //        //                rddResult = rddResult.repartitionAndSortWithinPartitions(partitionerByK2)
        //        //
        //        val rddResult = rdd.mapPartitionsWithIndex((pIdx, iter) => {
        //
        //            var qTreeDS1: QuadTree = null
        //            //            var qTreeDS2: QuadTree = null
        //            var ds1Row: (KeyBase, Any) = null
        //
        //            iter.map(row => {
        //
        //                row._1 match {
        //                    case _: Key0 => {
        //
        //                        ds1Row = row
        //
        //                        qTreeDS1 = row._2 match { case qt: QuadTree => qt }
        //
        //                        if (iter.hasNext)
        //                            null
        //                        else
        //                            Iterator(ds1Row)
        //                    }
        //                    case _: Key1 => {
        //
        //                        var ds2Row: (KeyBase, Any) = null
        //
        //                        val (gmGeom, gmGeomSet) = row._2.asInstanceOf[(GMGeomBase, SortSetObj)]
        //
        //                        QTreeOperations.nearestNeighbor(qTreeDS1, kParam, gmGeom, gmGeomSet)
        //
        //                        ds2Row = (row._1, (gmGeom, gmGeomSet))
        //
        //                        if (iter.hasNext)
        //                            Iterator(ds2Row)
        //                        else {
        //
        //                            // refine DS1 matches
        //                            QTreeOperations.refineMatchesDS1(qTreeDS1, kParam)
        //
        //                            Iterator(ds1Row, ds2Row)
        //                        }
        //                    }
        //                }
        //            })
        //                .filter(_ != null)
        //                .flatMap(_.seq)
        //        }, true)
        //
        //        rddResult.mapPartitions(_.map(row =>
        //            row._2 match {
        //                case qTree: QuadTree => {
        //
        //                    println(">>" + row)
        //                    qTree.getAllPoints
        //                        .flatMap(_.map(_.getUserData.asInstanceOf[(GMGeomBase, SortSetObj)]))
        //                        .iterator
        //                }
        //                case _ => {
        //
        //                    val data = row._2.asInstanceOf[(GMGeomBase, SortSetObj)]
        //                    Iterator((data._1, data._2))
        //                }
        //            })
        //            .flatMap(_.seq))
        //            .mapPartitions(_.map(row => "%s,%.8f,%.8f%s".format(row._1.payload, row._1.coordArr(0)._1, row._1.coordArr(0)._2, row._2.toString())))
        //            .saveAsTextFile(outDir, classOf[GzipCodec])

        if (clArgs.getParamValueBoolean(SparkKNN_Arguments.local)) {

            printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)

            println(outDir)
        }
    }

    import scala.collection.JavaConversions._
    private def getTreeItems(node: AbstractNode, lst: ListBuffer[(GMGeomBase, SortSetObj)]) {

        node.getChildBoundables.foreach(item => {

            item match {
                case an: AbstractNode => getTreeItems(an, lst)
                case ib: ItemBoundable => lst.append(ib.getItem().asInstanceOf[(GMGeomBase, SortSetObj)])
            }
        })
    }

    //    private def getClosestRegion(pointXY: (Double, Double), lstGlobalIndex: ListBuffer[ListBuffer[Region]], maxPartRows: Long) = {
    //        lstGlobalIndex.map(_.map(region => (Helper.squaredDist(region.center, pointXY), region)))
    //            .flatMap(_.seq)
    //            .sortBy(_._1)
    //
    //    }

    private def getIteratorSize(iter: Iterator[_]) = {
        var rowCount = 0L
        var maxRowSize = Int.MinValue
        while (iter.hasNext) {

            rowCount += 1
            val len = iter.next() match { case line: String => line.length }
            if (len > maxRowSize) maxRowSize = len
        }

        (maxRowSize, rowCount)
    }

    //    def printQT(qTree: QuadTree): Unit = {
    //
    //        var lst = ListBuffer[(Int, String)]()
    //
    //        printQT(qTree, 0, "")
    //        //
    //        //        lst = lst.sortBy(-_._1)
    //        //
    //        //        var height = lst.head._1
    //        //        //        var indent = level / 2
    //        //
    //        //        (lst.size - 1 to 0 by -1).foreach(idx => {
    //        //
    //        //            if (height != lst(idx)._1)
    //        //                println()
    //        //            else
    //        //                print('-')
    //        //
    //        //            (0 to height).foreach(i => print('-'))
    //        //
    //        //            height -= 1
    //        //
    //        //            print(lst(idx)._2)
    //        //        })
    //    }
    //
    //    def printQT(qTree: QuadTree, level: Int, spacer: String): Unit = {
    //
    //        if (qTree != null) {
    //
    //            print("|" + spacer)
    //            println("(%d, %d)".format(qTree.points.size, qTree.totalPoints))
    //
    //            printQT(qTree.topLeft, level + 1, spacer + "-")
    //            printQT(qTree.topRight, level + 1, spacer + "-")
    //            printQT(qTree.bottomLeft, level + 1, spacer + "-")
    //            printQT(qTree.bottomRight, level + 1, spacer + "-")
    //
    //            //            val sb = StringBuilder.newBuilder
    //            //
    //            //            sb.append((0 to level).map(i => "\t").mkString(""))
    //            //            sb.append("(%d, %d)".format(qTree.getPoints.size, qTree.getTotalPointCount))
    //            //            lst.append((level, "(%d, %d)".format(qTree.getPoints.size, qTree.getTotalPointCount)))
    //            //            print("|")
    //        }
    //    }

    //    private def getBestPartition(arrQTSummary: Array[(Int, QuadTreeSummary)], coordXY: (Double, Double)) = {
    //        val arr = arrQTSummary.filter(_._2.boundary.containsPoint(Point(coordXY._1, coordXY._2)))
    //            .map(summary => {
    //
    //                val dist = Helper.squaredDist((coordXY._1, coordXY._2), (summary._2.boundary.center.x, summary._2.boundary.center.y))
    //
    //                (dist, summary)
    //            })
    //
    //        if (arr.length == 0)
    //            -1
    //        else
    //            arr.minBy(_._1)._2._1
    //    }
}