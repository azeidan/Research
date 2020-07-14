package org.cusp.bdi.sknn

import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.SortedSet
import scala.util.Random

import org.apache.spark.Partitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.DistanceMeasure
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.serializer.KryoSerializer
import org.cusp.bdi.gm.GeoMatch
import org.cusp.bdi.gm.geom.GMGeomBase
import org.cusp.bdi.gm.geom.GMPoint
import org.cusp.bdi.sknn.util.RDD_Store
import org.cusp.bdi.sknn.util.SparkKNN_Arguments
import org.cusp.bdi.sknn.util.SparkKNN_Local_CLArgs
import org.cusp.bdi.util.Helper
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.index.strtree.AbstractNode
import org.locationtech.jts.index.strtree.ItemBoundable
import org.locationtech.jts.index.strtree.STRtree
import org.apache.hadoop.io.compress.GzipCodec
import org.cusp.bdi.sknn.util.STRtreeOperations
import breeze.linalg.functions.euclideanDistance
import scala.collection.mutable.Set
import org.cusp.bdi.sknn.util.HilbertIndex_V2
import org.cusp.bdi.gm.geom.util.LineRasterization

case class RowWrapper(_k: Int, _pointXY: (String, String)) extends Serializable with Ordered[RowWrapper] {

    def pointXY = _pointXY
    def k = _k

    override def compare(that: RowWrapper): Int =
        this.pointXY._1.toDouble.compareTo(that.pointXY._1.toDouble)
}

case class Region(xy1: (Double, Double), xy2: (Double, Double)) extends Serializable /*with Ordered[MBR]*/ {

    var minX = xy1._1 // initialPoint._1
    var minY = xy1._2 // initialPoint._2
    var maxX = xy2._1 // initialPoint._1
    var maxY = xy2._2 // initialPoint._2

    def this() =
        this((Double.PositiveInfinity, Double.PositiveInfinity), (Double.NegativeInfinity, Double.NegativeInfinity))

    var pointCount = 0L
    var assignedPart = -1
    val setHilbert = Set[Long]()

    //    def this() =
    //        this((Double.PositiveInfinity, Double.PositiveInfinity), (Double.NegativeInfinity, Double.NegativeInfinity))

    //    override def compare(that: MBR): Int =
    //        this.assignedPart.compare(that.assignedPart)

    //    override def equals(that: Any) = that match {
    //        case x: MBR => this.assignedPart.equals(x.assignedPart)
    //        case _ => false
    //    }
    //    def width = maxX - minX
    //    def height = maxY - minY
    //    var center = math.max(maxX - minX, maxY - minY)

    //    def contains(pointXY: (Double, Double)) =
    //        pointXY._1 >= minX && pointXY._1 <= maxX && pointXY._2 >= minY && pointXY._2 <= maxY

    def diffFromCenter(pointXY: (Double, Double)) =
        (math.abs(center._1 - pointXY._1), math.abs(center._2 - pointXY._2))

    def add(pointXY: (Double, Double), hilbertN: Long) {

        val hIdx = HilbertIndex_V2.computeIndex(hilbertN, ((pointXY._1 % hilbertN).toLong, (pointXY._2 % hilbertN).toLong))

        if (hIdx < 0)
            throw new Exception("Got negative hilbert index n=%d, xy=%s".format(hilbertN, pointXY.toString()))

        if (!setHilbert.contains(hIdx)) {

            setHilbert.add(hIdx)

            minX = math.min(minX, pointXY._1)
            minY = math.min(minY, pointXY._2)

            maxX = math.max(maxX, pointXY._1)
            maxY = math.max(maxY, pointXY._2)
        }

        pointCount += 1
    }

    def width = maxX - minX

    def height = maxY - minY

    def center =
        (minX + (width / 2), minY + (height / 2))

    def get() =
        ((minX, minY, maxX, maxY))

    //    def intersects(other: Region) =
    //        other.contains((minX, minY)) ||
    //            other.contains((maxX, minY)) ||
    //            other.contains((maxX, maxY)) ||
    //            other.contains((minX, maxY))

    override def toString() =
        "%d\t%.8f\t%.8f\t%.8f\t%.8f\t%d".format(assignedPart, minX, minY, maxX, maxY, pointCount /*, setHilbert.mkString(",")*/ )
}

//case class SubRegionCounts(_ll: Long = 0, _lr: Long = 0, _ur: Long = 0, _ul: Long = 0) {
//
//    var ll = _ll
//    var lr = _lr
//    var ur = _ur
//    var ul = _ul
//
//    def sum() = ll + lr + ur + ul
//
//    override def toString() =
//        "%d\t%d\t%d\t%d".format(ll, lr, ur, ul)
//}

object SparkKNN {

    def main(args: Array[String]): Unit = {

        val startTime = System.currentTimeMillis()
        var startTime2 = startTime
        val hilbertN = math.pow(2, 31).toLong

        val clArgs = SparkKNN_Local_CLArgs.randomPoints_randomPoints
        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift
        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint
        //        val clArgs = CLArgsParser(args, SparkKNN_Arguments())

        val sparkConf = new SparkConf()
            .setAppName(this.getClass.getName)
            .set("spark.serializer", classOf[KryoSerializer].getName)
            .registerKryoClasses(GeoMatch.getGeoMatchClasses())
            .registerKryoClasses(Array(classOf[KeyBase]))

        if (clArgs.getParamValueBoolean(SparkKNN_Arguments.local))
            sparkConf.setMaster("local[*]")

        val sc = new SparkContext(sparkConf)

        val kParam = clArgs.getParamValueInt(SparkKNN_Arguments.k)
        //        val numIter = clArgs.getParamValueInt(SparkKNN_Arguments.numIter)
        val minPartitions = clArgs.getParamValueInt(SparkKNN_Arguments.minPartitions)
        val outDir = clArgs.getParamValueString(SparkKNN_Arguments.outDir)

        // delete output dir if exists
        Helper.delDirHDFS(sc, clArgs.getParamValueString(SparkKNN_Arguments.outDir))

        val rddDS1Plain = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)
            .mapPartitions(_.map(RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj))))
        //            .reduceByKey((x, y) => x) // get rid of duplicates
        //            .persist()

        val numParts = rddDS1Plain.getNumPartitions
        val maxPartRows = (sc.runJob(rddDS1Plain, getIteratorSize _, Array(0, numParts / 2, numParts - 1)).sum / 3)

        var rddDS1_XY = rddDS1Plain
            .mapPartitions(_.map(row => (row._2._1, row._2._2)))
            //            .reduceByKey(_ + _) // eliminate duplicates
            //            .repartitionAndSortWithinPartitions(new HashPartitioner(numParts)) // group along the x-axis
            .persist()

        val kmeansK = numParts

        var kmeans = new KMeans().setK(kmeansK).setSeed(1L).setDistanceMeasure(DistanceMeasure.EUCLIDEAN)
        var kmModel = kmeans.run(rddDS1_XY.mapPartitions(_.map(row => Vectors.dense(row._1.toDouble, row._2.toDouble))))

        //        val kmModelCenters = new Array[(Int, (Double, Double))](kmModel.clusterCenters.length)
        //        val arrKMModelCenters = (0 until kmModel.clusterCenters.length)
        //            .map(i => (i, kmModel.clusterCenters(i)))
        kmeans = null
        //        kmModel = null

        //        (0 until kmModel.clusterCenters.length).foreach(x=>println(x + "\t" + kmModel.clusterCenters(x)))

        println(">T>Kmeans: %f".format((System.currentTimeMillis() - startTime2) / 1000.0))

        startTime2 = System.currentTimeMillis()

        //        var mapKtoPart = (0 until kmModel.clusterCenters.length).map(i => (i, i)).toMap

        val partitionerByK = new Partitioner() {

            override def numPartitions = kmeansK
            override def getPartition(key: Any): Int =
                key match {
                    case keyBase: KeyBase => keyBase.k
                    case rowWrapper: RowWrapper => rowWrapper.k
                    case k: Int => k
                }
        }

        var arrRegion = rddDS1_XY.mapPartitions(_.map(row => {

            val k = kmModel.predict(Vectors.dense(row._1.toDouble, row._2.toDouble))

            //            if (k == 3)
            //                println(">>%d\t%s\t%s".format(k, row._1, row._2))

            (RowWrapper(k, row), null)
        }))
            .repartitionAndSortWithinPartitions(partitionerByK)
            .mapPartitionsWithIndex((pIdx, iter) => {

                var rowK = -1
                val retList = ListBuffer[Region]()

                iter.foreach(row => {

                    val pointXY = (row._1.pointXY._1.toDouble, row._1.pointXY._2.toDouble)

                    if (rowK == -1)
                        rowK = row._1.k

                    if (rowK != row._1.k)
                        throw new Exception("Found more than one K on the same partition. Should not happen at this point (k==numParts). (%d and %d)".format(rowK, row._1))

                    if (retList.isEmpty /*|| (retList.last.pointCount >= maxPartRows && !retList.last.contains(pointXY))*/ ) {

                        val region = new Region()

                        if (retList.isEmpty)
                            region.assignedPart = rowK
                        else // for load balancing
                            region.assignedPart = -rowK // for load balancing

                        retList.append(region)
                    }

                    retList.last.add(pointXY, hilbertN)
                })

                retList.iterator
            })
            .collect

        rddDS1_XY.unpersist(false)
        rddDS1_XY = null

        //        arrRegion.foreach(row => println(">>%s".format(row)))

        val lstGlobalIndex: ListBuffer[ListBuffer[Region]] = ListBuffer.fill(numParts) { null }

        arrRegion.foreach(region =>
            if (region.assignedPart >= 0)
                lstGlobalIndex(region.assignedPart) = ListBuffer(region))

        val adjustedMaxPartRows = math.ceil(maxPartRows * 1.1).toInt // allow 10% increase in max rows to favor proximity

        (0 until arrRegion.length).foreach(i => {

            val region = arrRegion(i)

            if (region.assignedPart < 0) {

                // find an under utilized partition.
                val openPart = (lstGlobalIndex.size - 1 to 0 by -1).map(i => {

                    val lst = lstGlobalIndex(i)

                    val sum = if (lst.size == 1) lst(0).pointCount else lst.map(_.pointCount).sum

                    (sum, lst)
                })
                    .filter(_._1 + region.pointCount <= adjustedMaxPartRows)
                    .take(1)

                if (openPart.size == 1) {

                    region.assignedPart = openPart(0)._2(0).assignedPart
                    openPart(0)._2.append(region)
                }
                else /*if (lstPartToMBR.size == numParts)*/ {

                    region.assignedPart = lstGlobalIndex.size
                    lstGlobalIndex.append(ListBuffer(region))
                }
            }
        })

        arrRegion = null

        //        lstGlobalIndex.map(lst => println(">>\t%s".format(lst.mkString("\t"))))

        val partitionerByK2 = new Partitioner() {

            override def numPartitions = lstGlobalIndex.size
            override def getPartition(key: Any): Int =
                key match {
                    case keyBase: KeyBase => keyBase.k
                    case k: Int => k
                }
        }

        val rddDS1 = rddDS1Plain
            .mapPartitionsWithIndex((pIdx, iter) => iter.map(row => {

                // Compute best partition

                val pointXY = (row._2._1.toDouble, row._2._2.toDouble)

                //                val k = kmModel.predict(Vectors.dense(pointXY._1, pointXY._2))

                val bestRegion = getClosestRegion(pointXY, lstGlobalIndex, maxPartRows)(0)

                val key: KeyBase = Key0(bestRegion._2.assignedPart)

                (key, row)
            }))
            .partitionBy(partitionerByK2)
            .mapPartitionsWithIndex((pIdx, iter) => {

                val sTRtree = new STRtree
                val jtsGeomFact = new GeometryFactory

                var row: (KeyBase, (String, (String, String))) = null

                while (iter.hasNext) {

                    row = iter.next

                    val gmGeom: GMGeomBase = new GMPoint(row._2._1, (row._2._2._1, row._2._2._2))

                    //                    if (gmGeom.payload.equalsIgnoreCase("Ra_801763") ||
                    //                        gmGeom.payload.equalsIgnoreCase("Ra_363965") ||
                    //                        gmGeom.payload.equalsIgnoreCase("Ra_105357"))
                    //                        println

                    val env = gmGeom.toJTS(jtsGeomFact)(0).getEnvelopeInternal

                    sTRtree.insert(env, (gmGeom, SortSetObj(kParam)))
                }

                val ret: (KeyBase, (STRtree, GMGeomBase, SortSetObj, ListBuffer[Int])) = (row._1, (sTRtree, null, null, null))

                Iterator(ret)
            }, true)
            .partitionBy(partitionerByK2)

        val rddDS2 = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), minPartitions)
            .mapPartitionsWithIndex((pIdx, iter) => {

                val lineParser = RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj))

                val jtsGeomFact = new GeometryFactory

                iter.map(line => {

                    val row = lineParser(line)

                    val pointXY = (row._2._1.toDouble, row._2._2.toDouble)

                    //                    if (row._1.equalsIgnoreCase("Rb_999346"))
                    //                        println

                    val gmGeom: GMGeomBase = new GMPoint(row._1, (row._2._1, row._2._2))

                    val lstCloseRegion = getClosestRegion(pointXY, lstGlobalIndex, maxPartRows)

                    val lstVisitParts = ListBuffer[Int]()

                    val width = lstCloseRegion(0)._2.width
                    val height = lstCloseRegion(0)._2.height

                    val xyLL = (pointXY._1 - width, pointXY._2 - height)
                    val xyUR = (pointXY._1 + width, pointXY._2 + height)
                    val xyLR = (xyUR._1, xyLL._2)
                    val xyUL = (xyLL._1, xyLR._2)

                    val pointRegion = new Region(xyLL, xyUR)

                    LineRasterization((xyLL._1.toInt, xyLL._2.toInt), (xyLR._1.toInt, xyLR._2.toInt))
                        .foreach(xy => pointRegion.setHilbert.add(HilbertIndex_V2.computeIndex(hilbertN, (xy._1, xy._2))))
                    LineRasterization((xyLR._1.toInt, xyLR._2.toInt), (xyUR._1.toInt, xyUR._2.toInt))
                        .foreach(xy => pointRegion.setHilbert.add(HilbertIndex_V2.computeIndex(hilbertN, (xy._1, xy._2))))
                    LineRasterization((xyUR._1.toInt, xyUR._2.toInt), (xyUL._1.toInt, xyUL._2.toInt))
                        .foreach(xy => pointRegion.setHilbert.add(HilbertIndex_V2.computeIndex(hilbertN, (xy._1, xy._2))))
                    LineRasterization((xyUL._1.toInt, xyUL._2.toInt), (xyLL._1.toInt, xyLL._2.toInt))
                        .foreach(xy => pointRegion.setHilbert.add(HilbertIndex_V2.computeIndex(hilbertN, (xy._1, xy._2))))

                    do {
                        // intersects with
                        (0 until lstCloseRegion.size).foreach(i => {

                            if (!lstVisitParts.contains(lstCloseRegion(i)._2.assignedPart)) {

                                val setHIdx = lstCloseRegion(i)._2.setHilbert
                                    .filter(hIdx => pointRegion.setHilbert.contains(hIdx))
                                    .take(1)

                                if (setHIdx.size > 0) {
                                    lstVisitParts.append(lstCloseRegion(i)._2.assignedPart)
                                    pointRegion.pointCount += lstCloseRegion(i)._2.pointCount
                                }
                            }

                            //                            if (pointRegion.pointCount < kParam)
                            //                                throw new Exception("Region expand not yet implemented")
                            //                                pointRegion.expandBy(expandBy)
                        })
                    } while (pointRegion.pointCount < kParam);

                    val partNum = lstVisitParts(0)
                    lstVisitParts.remove(0)

                    val ret: (KeyBase, (STRtree, GMGeomBase, SortSetObj, ListBuffer[Int])) = (Key1(partNum), (null, gmGeom, SortSetObj(kParam), lstVisitParts))

                    println(">>%s\t%s\t%s".format(ret._1, ret._2._2.payload, ret._2._4.mkString("\t")))

                    ret
                })
            })
            .partitionBy(partitionerByK2)

        var rddResult = rddDS1.union(rddDS2)
            .saveAsTextFile(outDir, classOf[GzipCodec])
        //        (0 until 8).foreach(i => {
        //
        //            if (i > 0)
        //                rddResult = rddResult.repartitionAndSortWithinPartitions(partitionerByK2)
        //
        //            rddResult = rddResult.mapPartitionsWithIndex((pIdx, iter) => {
        //
        //                var sTRtree: STRtree = null
        //                var rTreeKey: KeyBase = null
        //                val jtsGeomFact = new GeometryFactory
        //                var ds1Row: (KeyBase, (STRtree, GMGeomBase, SortSetObj, ListBuffer[Int])) = null
        //
        //                iter.map(row => {
        //
        //                    row._1 match {
        //                        case _: Key0 => {
        //
        //                            ds1Row = row
        //
        //                            sTRtree = row._2._1 match { case rt: STRtree => rt }
        //                            rTreeKey = row._1
        //
        //                            if (iter.hasNext)
        //                                null
        //                            else
        //                                Iterator(ds1Row)
        //                        }
        //                        case _: Key1 => {
        //
        //                            var ds2Row: (KeyBase, (STRtree, GMGeomBase, SortSetObj, ListBuffer[Int])) = null
        //
        //                            val (_, gmGeom, gmGeomSet, closestParts) = row._2
        //
        //                            if (closestParts == null)
        //                                ds2Row = row
        //                            else {
        //
        //                                if (sTRtree != null)
        //                                    STRtreeOperations.rTreeNearestNeighbor(jtsGeomFact, gmGeom, gmGeomSet, kParam, sTRtree)
        //
        //                                if (closestParts.size == 0)
        //                                    ds2Row = (row._1, (null, gmGeom, gmGeomSet, null))
        //                                else {
        //
        //                                    row._1.k = closestParts(0)
        //                                    closestParts.remove(0)
        //                                    ds2Row = (row._1, (null, gmGeom, gmGeomSet, closestParts))
        //                                }
        //                            }
        //
        //                            if (iter.hasNext)
        //                                Iterator(ds2Row)
        //                            else
        //                                Iterator(ds1Row, ds2Row)
        //                        }
        //                    }
        //                })
        //                    .filter(_ != null)
        //                    .flatMap(_.seq)
        //            }, true)
        //        })
        //
        //        rddResult.mapPartitions(_.map(row => {
        //
        //            row._2._1 match {
        //                case sTRtree: STRtree => {
        //
        //                    val lst = ListBuffer[(GMGeomBase, SortSetObj)]()
        //
        //                    getTreeItems(sTRtree.getRoot, lst)
        //
        //                    lst.iterator
        //                }
        //                case _ => Iterator((row._2._2, row._2._3))
        //            }
        //        })
        //            .flatMap(_.seq))
        //            .mapPartitions(_.map(row => "%s,%s,%s%s".format(row._1.payload, row._1.coordArr(0)._1, row._1.coordArr(0)._2, row._2.toString())))
        //            .saveAsTextFile(outDir, classOf[GzipCodec])

        printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)

        println(outDir)
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

    private def getClosestRegion(pointXY: (Double, Double), lstGlobalIndex: ListBuffer[ListBuffer[Region]], maxPartRows: Long) = {
        lstGlobalIndex.map(_.map(region => (Helper.squaredDist(region.center, pointXY), region)))
            .flatMap(_.seq)
            .sortBy(_._1)

    }

    private def getIteratorSize(iter: Iterator[_]) = {
        var count = 0
        var size = 0
        while (iter.hasNext) {
            count += 1
            iter.next()
        }
        count
    }
}