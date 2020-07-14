package org.cusp.bdi.sknn

import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.gm.GeoMatch
import org.cusp.bdi.gm.geom.GMGeomBase
import org.cusp.bdi.gm.geom.GMPoint
import org.cusp.bdi.sknn.util.AssignToPartitions
import org.cusp.bdi.sknn.util.QTreeOperations
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
import scala.collection.immutable.SortedSet

case class Region() extends Serializable /*with Ordered[MBR]*/ {

    var uniqueID: String = null
    var pointCount = 0L
    var assignedPart = "-1"
    var minX = Double.MaxValue
    var minY = Double.MaxValue
    var maxX = Double.MinValue
    var maxY = Double.MinValue

    def contains(pointXY: (Double, Double)) =
        pointXY._1 >= minX && pointXY._1 <= maxX && pointXY._2 >= minY && pointXY._2 <= maxY

    def diffFromCenter(pointXY: (Double, Double)) =
        (math.abs(center._1 - pointXY._1), math.abs(center._2 - pointXY._2))

    def add(pointXY: (Double, Double)) {

        if (pointXY._1 < minX) minX = pointXY._1
        if (pointXY._2 < minY) minY = pointXY._2
        if (pointXY._1 > maxX) maxX = pointXY._1
        if (pointXY._2 > maxY) maxY = pointXY._2

        pointCount += 1
    }

    def width = maxX - minX

    def height = maxY - minY

    def center =
        (minX + (width / 2), minY + (height / 2))

    def get() =
        ((minX, minY, maxX, maxY))

    override def toString() =
        "%s\t%.8f\t%.8f\t%.8f\t%.8f\t%d".format(assignedPart, minX, minY, maxX, maxY, pointCount)
    //        "%s\t%.8f\t%.8f\t%.8f\t%.8f\t%d".format(assignedPart, minX, minY, maxX, maxY, pointCount)

    //    def expandBy(expandBy: Double) = {
    //        minX -= expandBy
    //        minY -= expandBy
    //        maxX += expandBy
    //        maxY += expandBy
    //    }
}

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
        val minPartitions = clArgs.getParamValueInt(SparkKNN_Arguments.minPartitions)
        val outDir = clArgs.getParamValueString(SparkKNN_Arguments.outDir)

        // delete output dir if exists
        Helper.delDirHDFS(sc, clArgs.getParamValueString(SparkKNN_Arguments.outDir))

        val rddDS1Lines = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)

        //        val execCores = sparkConf.getInt("spark.executor.cores", 1)

        // 7% reduction in memory to account for overhead operations
        var execMem = Helper.toByte(sparkConf.get("spark.executor.memory", sc.getExecutorMemoryStatus.map(_._2._1).max + "B")) // skips memory of core assigned for Hadoop daemon
        // deduct yarn overhead
        execMem -= math.ceil(math.max(384, 0.07 * execMem)).toLong

        val info = sc.runJob(rddDS1Lines, getIteratorSize _, Array(0, rddDS1Lines.getNumPartitions / 2, rddDS1Lines.getNumPartitions - 1))
            .fold((0, 0L))((x, y) => (x._1 + y._1, x._2 + y._2))

        val (maxRowSize, partRowCount) = (math.ceil(info._1 / 3.0).toInt, math.ceil(info._2 / 3.0).toInt)

        val totalRowCount = partRowCount * rddDS1Lines.getNumPartitions

        val gmGeomDummy = GMPoint((0 until maxRowSize).map(_ => " ").mkString(""), (0, 0))
        val pointDummy = new Point(0, 0)
        val qtEmptyDummy = new QuadTree(Box(pointDummy.clone, pointDummy.clone))
        val sSetDummy = SortSetObj(kParam)
        //        (0 until kParam).foreach(i => sSetDummy.add((i, gmGeomDummy.clone())))
        //        val geomSetDummy = (gmGeomDummy, sSetDummy)

        val geomSetCost = SizeEstimator.estimate(gmGeomDummy)
        // GMGeom and matches cost (= GMGeom + SortedSet + (K * GMGeom in SortedSet))
        val geomMatchesCost = geomSetCost + SizeEstimator.estimate(sSetDummy) + (kParam * geomSetCost)
        // pointCost = point coords + GMGeom and matches cost
        val pointCost = SizeEstimator.estimate(pointDummy) + geomMatchesCost
        val qTreeCost = SizeEstimator.estimate(qtEmptyDummy)
        // *2 to account for qTree operational overhead of Points
        var execRowCapacity = ((execMem - qTreeCost - geomMatchesCost) / pointCost).toLong

        // To-do account for pointers to points

        var numParts = math.ceil(totalRowCount.toDouble / execRowCapacity).toInt

        if (numParts == 1) {

            numParts = rddDS1Lines.getNumPartitions
            execRowCapacity = totalRowCount / numParts
        }

        //        println("<<execMem=" + execMem)
        //        println("<<maxRowSize=" + maxRowSize)
        //        println("<<rowCount=" + rowCount)
        //        println("<<totalRowCount=" + totalRowCount)
        //        println("<<geomSetCost=" + geomSetCost)
        //        println("<<qTreeCost=" + qTreeCost)
        //        println("<<adjustedRowsPerExec=" + adjustedRowsPerExec)
        //        println("<<numParts=" + numParts)

        //        // X-axis range
        //        var ds1RangeX = rddDS1Lines
        //            .mapPartitions(_.map(RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj))))
        //            .mapPartitions(iter => Iterator(iter.map(row => (row._2._1.toDouble, row._2._1.toDouble))
        //                .fold((Double.MaxValue, Double.MinValue))((x, y) =>
        //                    (math.min(x._1, y._1), math.max(x._2, y._2)))))
        //            .fold((Double.MaxValue, Double.MinValue))((x, y) =>
        //                (math.min(x._1, y._1), math.max(x._2, y._2)))

        val bucketSize = math.ceil(Int.MaxValue / rddDS1Lines.getNumPartitions.toDouble).toInt

        val partitionerX = new Partitioner() {

            override def numPartitions = rddDS1Lines.getNumPartitions //math.ceil((ds1RangeX._2 - ds1RangeX._1) / execRowCapacity).toInt
            override def getPartition(key: Any): Int =
                key.asInstanceOf[((Double, Double), Int)]._2
        }

        val arrQuads = rddDS1Lines
            .mapPartitions(_.map(RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj))))
            .mapPartitions(_.map(row => {

                //                val x = Helper.floorNumStr(row._2._1).toInt
                //                val y = Helper.floorNumStr(row._2._2).toInt

                val xy = (row._2._1.toDouble, row._2._2.toDouble)

                val key = math.floor(xy._1 / bucketSize).toInt

                ((xy, key), null)
            }))
            //            .reduceByKey((y1, y2) => if (y1 == y2) y1 else y1 ++ y2)
            //            .mapPartitions(iter => {
            //
            //                iter.map(_._1).to[TreeSet]
            //
            //                iter.map(row => {
            //
            //                    val xy = (row._2._1.toDouble, row._2._2.toDouble)
            //
            //                    //                    val key = math.floor((xy._1 - ds1RangeX._1) / execRowCapacity).toInt
            //                    val key = math.floor(xy._1 / bucketSize).toInt
            //
            //                    //                    ((xy, key), null)
            //                    (key, xy)
            //                })
            //            })
            .repartitionAndSortWithinPartitions(partitionerX)
            .mapPartitionsWithIndex((pIdx, iter) => {

                val lstQT = ListBuffer[QuadTree]()

                def createNewQT() {

                    val widthHalf = Double.MaxValue / 2 // ((ds1RangeX._2 - ds1RangeX._1) / 2) + 0.1
                    val heightHalf = widthHalf //((maxY - minY) / 2) + 0.1
                    val qTree = new QuadTree(Box(new Point(widthHalf, heightHalf), new Point(widthHalf, heightHalf)))

                    lstQT.append(qTree)
                }

                iter.map(_._1._1)
                    .foreach(xy => {

                        //                    println("<<\t%d\t%d\t%d".format(pIdx, xy._1, xy._2))

                        if (lstQT.isEmpty || lstQT.last.totalPoints == execRowCapacity) {

                            if (!lstQT.isEmpty)
                                lstQT.last.summarize()

                            createNewQT()
                        }

                        //                        val xy = row._1._1

                        lstQT.last.insert(new Point(xy._1, xy._2))
                    })

                if (!lstQT.isEmpty)
                    lstQT.last.summarize()

                lstQT.iterator
            })
            .collect()

        AssignToPartitions(arrQuads, execRowCapacity, 0)

        arrQuads.foreach(qTree => println(">>\t%d\t%d\t%s".format(SizeEstimator.estimate(qTree), qTree.assignedPart, qTree.getInnerBoundary)))
        //        println(">>\t" + ds1RangeX._1 + "\t" + ds1RangeX._2)

        //        startTime2 = System.currentTimeMillis()

        //        val partitionerExtract = new Partitioner() {
        //
        //            override def numPartitions = arrQuads.length
        //            override def getPartition(key: Any): Int =
        //                key match {
        //                    case k: Int => k
        //                    case kBase: KeyBase => if (kBase.partId == -1) numParts - 1 else kBase.partId
        //                }
        //        }
        //
        //        var rangesX = arrQuads.map(qTree =>
        //            (qTree.assignedPart, math.floor(qTree.getInnerBoundary.center.x - qTree.getInnerBoundary.halfDimension.x).toLong, math.ceil(qTree.getInnerBoundary.center.x + qTree.getInnerBoundary.halfDimension.x).toLong))
        //
        //        val rddDS1 = rddDS1Lines
        //            .mapPartitions(_.map(RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj))))
        //            .mapPartitions(_.map(row => {
        //
        //                val coordXY = (row._2._1.toDouble, row._2._2.toDouble)
        //
        //                // val kMeansK = kmModel.predict(Vectors.dense(x, y))
        //
        //                val partNum = getBestPartition(rangesX, coordXY._1)
        //
        //                //                if (coordXY._1.toString().startsWith("1004588"))
        //                //                    getBestPartition(rangesX, coordXY._1)
        //
        //                if (partNum == -1)
        //                    throw new Exception("Unhandeled state: point from DS1 isn't contained in any of the regions: " + coordXY)
        //
        //                (partNum, row)
        //            }))
        //            .partitionBy(partitionerExtract)
        //            .mapPartitionsWithIndex((pIdx, iter) => {
        //
        //                val lstPoints = ListBuffer[Point]()
        //                var minX = Double.MaxValue
        //                var minY = Double.MaxValue
        //                var maxX = Double.MinValue
        //                var maxY = Double.MinValue
        //
        //                iter.foreach(row => {
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
        //                val qTree = new QuadTree(Box(new Point(widthHalf + minX, heightHalf + minY), new Point(widthHalf + 0.1, heightHalf + 0.1)))
        //
        //                qTree.insert(lstPoints)
        //
        //                val key0: KeyBase = Key0(pIdx)
        //                val ret: (KeyBase, Any) = (key0, qTree)
        //
        //                //                println(">>\t%s\t%f\t%f\t%f\t%f\t%d".format(key0.partId, qTree.boundary.left, qTree.boundary.bottom, qTree.boundary.right, qTree.boundary.top, qTree.getTotalPoints))
        //                //                println(">>>\t%s\t%s\t%s\t%s\t%s".format(key0.partId, tmpMinX, tmpMinY, tmpMaxX, tmpMaxY))
        //
        //                Iterator(ret)
        //            }, true)
        //        //            .saveAsTextFile(outDir, classOf[GzipCodec])
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
        //                    val partNum = getBestPartition(rangesX, coordXY._1)
        //
        //                    //                    if (coordXY._1.toString().startsWith("1031137"))
        //                    //                        getBestPartition(rangesX, coordXY._1)
        //
        //                    if (partNum == -1)
        //                        throw new Exception("Unhandeled state: point from DS2 isn't contained in any of the regions: " + coordXY)
        //
        //                    //                    (partNum, row)
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
        //            .partitionBy(partitionerExtract)
        //
        //        var rdd = rddDS1.union(rddDS2)
        //
        //        //        println(rdd.toDebugString)
        //
        //        //        //        (0 until 8).foreach(i => {
        //        //
        //        //        //            if (i > 0)
        //        //        //                rddResult = rddResult.repartitionAndSortWithinPartitions(partitionerByK2)
        //
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
        //                        .flatMap(_.map(_.userData.asInstanceOf[(GMGeomBase, SortSetObj)]))
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

    def getBestPartition(rangesX: Array[(Int, Long, Long)], pointX: Double): Int = {

        //        var pIdx = binarySearch(rangesX, pointX)
        //
        //        // approximate
        //        if (pIdx == -1) {
        //
        //            (0 until rangesX.length).foreach(counter =>
        //                if (pointX < rangesX(counter)._3)
        //                    return rangesX(counter)._1)
        //
        //            return rangesX.last._1
        //        }
        //
        //        pIdx

        var topIdx = 0
        var botIdx = rangesX.length - 1
        var approximate = -1

        if (pointX < rangesX.head._2)
            return rangesX.head._1
        else if (pointX > rangesX.last._2)
            return rangesX.last._1
        else
            while (botIdx >= topIdx) {

                val midIdx = (topIdx + botIdx) / 2
                val bucket = rangesX(midIdx)

                if (!(pointX < bucket._2 || pointX > bucket._3))
                    return bucket._1
                else if (pointX < bucket._2) {

                    approximate = bucket._1

                    botIdx = midIdx - 1
                }
                else
                    topIdx = midIdx + 1
            }

        approximate
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

}