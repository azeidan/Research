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

case class RowWrapper(_k: Int, _pointXY: (String, String)) extends Serializable with Ordered[RowWrapper] {

    def pointXY = _pointXY
    def k = _k

    override def compare(that: RowWrapper): Int =
        this.pointXY._1.toDouble.compareTo(that.pointXY._1.toDouble)
}

case class MBR(_minX: Double = Double.PositiveInfinity,
               _minY: Double = Double.PositiveInfinity,
               _maxX: Double = Double.NegativeInfinity,
               _maxY: Double = Double.NegativeInfinity) extends Serializable with Ordered[MBR] {

    var pointCount = 0L
    var assignedPart = -1

    var minX = _minX
    var minY = _minY
    var maxX = _maxX
    var maxY = _maxY

    override def compare(that: MBR): Int =
        this.assignedPart.compare(that.assignedPart)

    override def equals(that: Any) = that match {
        case x: MBR => this.assignedPart.equals(x.assignedPart)
        case _ => false
    }

    def centerDisplacement(pointXY: (Double, Double)) = {

        val cent = this.center
        (pointXY._1 - cent._1, pointXY._2 - cent._2)
    }

    def add(pointXY: (Double, Double)) {

        minX = math.min(minX, pointXY._1)
        minY = math.min(minY, pointXY._2)

        maxX = math.max(maxX, pointXY._1)
        maxY = math.max(maxY, pointXY._2)

        pointCount += 1
    }

    def add(other: MBR) {

        this.add((other.minX, other.minY))
        this.add((other.maxX, other.maxY))

        this.pointCount += other.pointCount
    }

    def width() = maxX - minX

    def height() = maxY - minY

    def center() =
        (minX + (width / 2), minY + (height / 2))

    def get() =
        ((minX, minY, maxX, maxY))

    def toGeometryJTS(jtsGeomFact: GeometryFactory) =
        jtsGeomFact.toGeometry(new Envelope(minX, maxX, minY, maxY))

    override def toString() =
        "%s\t%.8f\t%.8f\t%.8f\t%.8f\t%d".format(assignedPart, minX, minY, maxX, maxY, pointCount)
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

            override def numPartitions = rddDS1Plain.getNumPartitions
            override def getPartition(key: Any): Int =
                key match {
                    case keyBase: KeyBase => keyBase.k
                    case rowWrapper: RowWrapper => rowWrapper.k
                    case k: Int => k
                }
        }

        var arrMBR = rddDS1_XY.mapPartitions(_.map(row =>
            (RowWrapper(kmModel.predict(Vectors.dense(row._1.toDouble, row._2.toDouble)), row), null)))
            .repartitionAndSortWithinPartitions(partitionerByK)
            .mapPartitionsWithIndex((pIdx, iter) => {

                var rowK = -1
                val retList = ListBuffer[MBR]()

                iter.foreach(row => {

                    val point = (row._1.pointXY._1.toDouble, row._1.pointXY._2.toDouble)

                    if (rowK == -1)
                        rowK = row._1.k

                    if (rowK != row._1.k)
                        throw new Exception("Found more than one K on the same partition. Should not happen at this point (k==numParts). (%d and %d)".format(rowK, row._1))

                    if (retList.isEmpty || (retList.last.pointCount >= maxPartRows && point._1 <= retList.last.maxX)) {

                        val mbr = MBR()

                        if (retList.isEmpty)
                            mbr.assignedPart = rowK
                        else
                            mbr.assignedPart = -rowK

                        retList.append(mbr)
                    }

                    retList.last.add(point)
                })

                retList.iterator
            })
            .collect

        rddDS1_XY.unpersist(false)
        rddDS1_XY = null

        //        arrMBR.foreach(row => println(">>%s".format(row)))

        val lstPartToMBR: ListBuffer[ListBuffer[MBR]] = ListBuffer.fill(numParts) { null }

        arrMBR.foreach(mbr =>
            if (mbr.assignedPart >= 0)
                lstPartToMBR(mbr.assignedPart) = ListBuffer(mbr))

        val adjustedMaxPartRows = math.ceil(maxPartRows * 1.1).toInt // allow 10% increase in max rows to favor proximity

        (0 until arrMBR.length).foreach(i => {

            val mbr = arrMBR(i)

            if (mbr.assignedPart < 0) {

                // find an under utilized partition.
                val openPart = (lstPartToMBR.size - 1 to 0 by -1).map(i => {

                    val lst = lstPartToMBR(i)

                    val sum = if (lst.size == 1) lst(0).pointCount else lst.map(_.pointCount).sum

                    (sum, lst)
                })
                    .filter(_._1 + mbr.pointCount <= adjustedMaxPartRows)
                    .take(1)

                if (openPart.size == 1) {

                    mbr.assignedPart = openPart(0)._2(0).assignedPart
                    openPart(0)._2.append(mbr)
                }
                else /*if (lstPartToMBR.size == numParts)*/ {

                    mbr.assignedPart = lstPartToMBR.size
                    lstPartToMBR.append(ListBuffer(mbr))
                }
            }
        })

        arrMBR = null

        //        lstPartToMBR.map(lst => println(">>\t%s".format(lst.mkString("\t"))))

        val partitionerByK2 = new Partitioner() {

            override def numPartitions = lstPartToMBR.size
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

                val bestMBR = getBestMBR(pointXY, lstPartToMBR, maxPartRows)(0)

                val key: KeyBase = Key0(bestMBR._2.assignedPart)

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

                    if (gmGeom.payload.equalsIgnoreCase("Ra_801763") ||
                        gmGeom.payload.equalsIgnoreCase("Ra_363965") ||
                        gmGeom.payload.equalsIgnoreCase("Ra_105357"))
                        println

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
                    if (row._1.equalsIgnoreCase("Rb_999346"))
                        println
                    //                    var k = kmModel.predict(Vectors.dense(pointXY._1, pointXY._2))
                    //                    val kMBR = lstPartToMBR(k)(0)
                    val lstBestMBR = getBestMBR(pointXY, lstPartToMBR, maxPartRows)

                    val gmGeom: GMGeomBase = new GMPoint(row._1, (row._2._1, row._2._2))

                    //                    var expandBy = math.min(lstBestMBR(0)._2.width, lstBestMBR(0)._2.height)
                    val lstVisitParts = ListBuffer(lstBestMBR(0)._2.assignedPart)
                    var pointsInRadius = lstBestMBR(0)._2.pointCount

                    val centDisp = lstBestMBR(0)._2.centerDisplacement(pointXY)
                    var pointMBR = MBR(lstBestMBR(0)._2.minX + centDisp._1, lstBestMBR(0)._2.minY + centDisp._2,
                        lstBestMBR(0)._2.maxX + centDisp._1, lstBestMBR(0)._2.maxY + centDisp._2)

                    val expandBy = math.min(pointMBR.width, pointMBR.height)

                    var jtsEnvGMGeom = pointMBR.toGeometryJTS(jtsGeomFact)

                    do {

                        // intersects with
                        (1 until lstBestMBR.size).foreach(i => {

                            if (!lstVisitParts.contains(lstBestMBR(i)._2.assignedPart)) {

                                val jtsMBR = lstBestMBR(i)._2.toGeometryJTS(jtsGeomFact)

                                if (jtsEnvGMGeom.intersects(jtsMBR)) {
                                    lstVisitParts.append(lstBestMBR(i)._2.assignedPart)
                                    pointsInRadius += lstBestMBR(i)._2.pointCount
                                }
                            }
                        })

                        if (pointsInRadius < kParam) {

                            val env = jtsEnvGMGeom.getEnvelopeInternal
                            env.expandBy(expandBy)
                            jtsEnvGMGeom = jtsGeomFact.toGeometry(env)
                        }
                    } while (pointsInRadius < kParam);

                    val partNum = lstVisitParts(0)
                    lstVisitParts.remove(0)

                    val ret: (KeyBase, (STRtree, GMGeomBase, SortSetObj, ListBuffer[Int])) = (Key1(partNum), (null, gmGeom, SortSetObj(kParam), lstVisitParts))

                    //                    println(">>%s\t%s\t%s".format(ret._1, ret._2._2.payload, ret._2._4.mkString("\t")))

                    ret
                })
            })
            .partitionBy(partitionerByK2)

        var rddResult = rddDS1.union(rddDS2)

        (0 until 8).foreach(i => {

            if (i > 0)
                rddResult = rddResult.repartitionAndSortWithinPartitions(partitionerByK2)

            rddResult = rddResult.mapPartitionsWithIndex((pIdx, iter) => {

                var sTRtree: STRtree = null
                var rTreeKey: KeyBase = null
                val jtsGeomFact = new GeometryFactory
                var ds1Row: (KeyBase, (STRtree, GMGeomBase, SortSetObj, ListBuffer[Int])) = null

                iter.map(row => {

                    row._1 match {
                        case _: Key0 => {

                            ds1Row = row

                            sTRtree = row._2._1 match { case rt: STRtree => rt }
                            rTreeKey = row._1

                            if (iter.hasNext)
                                null
                            else
                                Iterator(ds1Row)
                        }
                        case _: Key1 => {

                            var ds2Row: (KeyBase, (STRtree, GMGeomBase, SortSetObj, ListBuffer[Int])) = null

                            val (_, gmGeom, gmGeomSet, closestParts) = row._2

                            if (closestParts == null)
                                ds2Row = row
                            else {

                                if (sTRtree != null)
                                    STRtreeOperations.rTreeNearestNeighbor(jtsGeomFact, gmGeom, gmGeomSet, kParam, sTRtree)

                                if (closestParts.size == 0)
                                    ds2Row = (row._1, (null, gmGeom, gmGeomSet, null))
                                else {

                                    row._1.k = closestParts(0)
                                    closestParts.remove(0)
                                    ds2Row = (row._1, (null, gmGeom, gmGeomSet, closestParts))
                                }
                            }

                            if (iter.hasNext)
                                Iterator(ds2Row)
                            else
                                Iterator(ds1Row, ds2Row)
                        }
                    }
                })
                    .filter(_ != null)
                    .flatMap(_.seq)
            }, true)
        })

        rddResult.mapPartitions(_.map(row => {

            row._2._1 match {
                case sTRtree: STRtree => {

                    val lst = ListBuffer[(GMGeomBase, SortSetObj)]()

                    getTreeItems(sTRtree.getRoot, lst)

                    lst.iterator
                }
                case _ => Iterator((row._2._2, row._2._3))
            }
        })
            .flatMap(_.seq))
            .mapPartitions(_.map(row => "%s,%s,%s%s".format(row._1.payload, row._1.coordArr(0)._1, row._1.coordArr(0)._2, row._2.toString())))
            .saveAsTextFile(outDir, classOf[GzipCodec])

        //        //        //        val itemDist = new ItemDistance() with Serializable {
        //        //        //            override def distance(item1: ItemBoundable, item2: ItemBoundable) = {
        //        //        //
        //        //        //                val jtsGeomFact = new GeometryFactory
        //        //        //                item1.getItem.asInstanceOf[(GMGeomBase, SortSetObj)]._1.toJTS(jtsGeomFact)(0).distance(
        //        //        //                    item2.getItem.asInstanceOf[Point])
        //        //        //            }
        //        //        //        }
        //        //        //
        //        //        //        var rdd = rddDS1.union(rddDS2)
        //        //        //            .mapPartitionsWithIndex((pIdx, iter) => {
        //        //        //
        //        //        //                val sTRtree = new STRtree
        //        //        //                val jtsGeomFact = new GeometryFactory
        //        //        //                var newKey1: KeyBase = null
        //        //        //                var sTRtreeKey: KeyBase = null
        //        //        //
        //        //        //                iter.map(row => {
        //        //        //
        //        //        //                    row._1 match {
        //        //        //                        case _: Key0 => {
        //        //        //
        //        //        //                            val env = row._2.toJTS(jtsGeomFact)(0).getEnvelopeInternal
        //        //        //
        //        //        //                            sTRtree.insert(env, (row._2, SortSetObj(kParam)))
        //        //        //
        //        //        //                            if (sTRtreeKey == null)
        //        //        //                                sTRtreeKey = row._1
        //        //        //
        //        //        //                            if (iter.hasNext)
        //        //        //                                null
        //        //        //                            else
        //        //        //                                Iterator((row._1, sTRtree))
        //        //        //                        }
        //        //        //                        case _: Key1 => {
        //        //        //
        //        //        //                            //                            if (row._2.payload.equalsIgnoreCase("B200463") ||
        //        //        //                            //                                row._2.payload.equalsIgnoreCase("B200321") ||
        //        //        //                            //                                row._2.payload.equalsIgnoreCase("B200066"))
        //        //        //                            //                                println()
        //        //        //
        //        //        //                            val (gmGeom, gmGeomSet) = (row._2, SortSetObj(kParam))
        //        //        //
        //        //        //                            if (gmGeom.payload.equalsIgnoreCase("rb_463922"))
        //        //        //                                println
        //        //        //
        //        //        //                            if (sTRtree != null)
        //        //        //                                STRtreeOperations.rTreeNearestNeighbor(jtsGeomFact, gmGeom, gmGeomSet, kParam * 5, sTRtree) // *5 for boundery objects
        //        //        //
        //        //        //                            val newKey1 = Key1(getNextPart(row._1.k, mapGlobalIndex, mapCenterInfo))
        //        //        //                            val ds2Row: (KeyBase, Any) = (newKey1, (gmGeom, gmGeomSet))
        //        //        //
        //        //        //                            if (iter.hasNext)
        //        //        //                                Iterator(ds2Row)
        //        //        //                            else
        //        //        //                                Iterator(ds2Row, (sTRtreeKey, sTRtree))
        //        //        //                        }
        //        //        //                    }
        //        //        //                })
        //        //        //                    .filter(_ != null)
        //        //        //                    .flatMap(_.seq)
        //        //        //            }, true)
        //        //        //
        //        //        //        // 2nd round
        //        //        //        val rddResult = rdd.repartitionAndSortWithinPartitions(partitionerByK)
        //        //        //            .mapPartitions(iter => {
        //        //        //
        //        //        //                var sTRtree: STRtree = null
        //        //        //                var rTreeKey: KeyBase = null
        //        //        //                val jtsGeomFact = new GeometryFactory
        //        //        //
        //        //        //                iter.map(row => {
        //        //        //
        //        //        //                    row._1 match {
        //        //        //                        case _: Key0 => {
        //        //        //
        //        //        //                            sTRtree = row._2 match { case rt: STRtree => rt }
        //        //        //                            rTreeKey = row._1
        //        //        //
        //        //        //                            if (iter.hasNext)
        //        //        //                                null
        //        //        //                            else
        //        //        //                                Iterator[Any](sTRtree)
        //        //        //                        }
        //        //        //                        case _: Key1 => {
        //        //        //
        //        //        //                            val (gmGeom, gmGeomSet) = row._2.asInstanceOf[(GMGeomBase, SortSetObj)]
        //        //        //
        //        //        //                            if (sTRtree != null)
        //        //        //                                STRtreeOperations.rTreeNearestNeighbor(jtsGeomFact, gmGeom, gmGeomSet, kParam, sTRtree)
        //        //        //
        //        //        //                            val ds2Row = (gmGeom, gmGeomSet)
        //        //        //
        //        //        //                            if (iter.hasNext)
        //        //        //                                Iterator(ds2Row)
        //        //        //                            else
        //        //        //                                Iterator[Any](sTRtree, ds2Row)
        //        //        //                        }
        //        //        //                    }
        //        //        //                })
        //        //        //
        //        //        //                    .filter(_ != null)
        //        //        //                    .flatMap(_.seq)
        //        //        //            }, true)
        //        //        //
        //        //        //        rddResult.mapPartitions(_.map(row => {
        //        //        //
        //        //        //            row match {
        //        //        //                case sTRtree: STRtree => {
        //        //        //
        //        //        //                    val lst = ListBuffer[(GMGeomBase, SortSetObj)]()
        //        //        //
        //        //        //                    getTreeItems(sTRtree.getRoot, lst)
        //        //        //
        //        //        //                    lst.iterator
        //        //        //                }
        //        //        //                case _ => Iterator(row.asInstanceOf[(GMGeomBase, SortSetObj)])
        //        //        //            }
        //        //        //        })
        //        //        //            .flatMap(_.seq))
        //        //        //            .mapPartitions(_.map(row => "%s,%.8f,%.8f%s".format(row._1.payload, row._1.coordArr(0)._1, row._1.coordArr(0)._2, row._2.toString())))
        //        //        //            .saveAsTextFile(outDir, classOf[GzipCodec])

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

    private def getBestMBR(pointXY: (Double, Double), lstPartToMBR: ListBuffer[ListBuffer[MBR]], maxPartRows: Long) = {
        lstPartToMBR.map(_.map(mbr => (Helper.squaredDist(mbr.center, pointXY), mbr)))
            .flatMap(_.seq)
            .sortBy(_._1)

    }

    //    private def euclideanDist(vect1: Vector, vect2: Vector) = {
    //
    //        val arr1 = vect1.toArray
    //        val arr2 = vect2.toArray
    //
    //        Helper.euclideanDist((arr1(0), arr1(1)), (arr2(0), arr2(1)))
    //    }

    private def getIteratorSize(iter: Iterator[_]) = {
        var count = 0
        var size = 0
        while (iter.hasNext) {
            count += 1
            iter.next()
        }
        count
    }

    //    private def mergeMBRs(mbr1: ((String, String), (String, String)), mbr2: ((String, String), (String, String))) = {
    //
    //        var (minX1, minY1) = (mbr1._1._1.toDouble, mbr1._1._2.toDouble)
    //        var (maxX1, maxY1) = (mbr1._2._1.toDouble, mbr1._2._2.toDouble)
    //
    //        var (minX2, minY2) = (mbr2._1._1.toDouble, mbr2._1._2.toDouble)
    //        var (maxX2, maxY2) = (mbr2._2._1.toDouble, mbr2._2._2.toDouble)
    //
    //        /*((math.min(minX1, minX2),
    //         *  math.min(minY1, minY2)),
    //         *  (math.max(maxX1, maxX2),
    //         *      math.max(maxY1, maxY2)))*/
    //
    //        (("%.8f".format(math.min(minX1, minX2)),
    //            "%.8f".format(math.min(minY1, minY2))),
    //            ("%.8f".format(math.max(maxX1, maxX2)),
    //                "%.8f".format(math.max(maxY1, maxY2))))
    //    }

    //    def computeMBR(sc: SparkContext, fileName: String, datasetObj: String, minPartitions: Int) = {
    //        RDD_Store.getRDDPlain(sc, fileName, minPartitions)
    //            .mapPartitionsWithIndex((pIdx, iter) => {
    //
    //                val lineParser = RDD_Store.getLineParser(datasetObj)
    //
    //                var (minX: Long, minY: Long, maxX: Long, maxY: Long) =
    //                    iter.map(lineParser)
    //                        .map(row => (row._2._1.toDouble, row._2._2.toDouble))
    //                        .fold((Long.MaxValue, Long.MaxValue, Long.MinValue, Long.MinValue))((x, y) => {
    //
    //                            val (a, b, c, d) = x.asInstanceOf[(Long, Long, Long, Long)]
    //                            val t = y.asInstanceOf[(Long, Long)]
    //
    //                            (math.min(a, t._1), math.min(b, t._2), math.max(c, t._1), math.max(d, t._2))
    //                        })
    //                Iterator((minX, minY, maxX, maxY))
    //            })
    //            .fold((Long.MaxValue, Long.MaxValue, Long.MinValue, Long.MinValue))((x, y) => {
    //
    //                val (a, b, c, d) = x.asInstanceOf[(Long, Long, Long, Long)]
    //                val t = y.asInstanceOf[(Long, Long, Long, Long)]
    //
    //                (math.min(a, t._1), math.min(b, t._2), math.max(c, t._3), math.max(d, t._4))
    //            })
    //    }

    //    def getNextPart(currK: Int, mapGlobalIndex: HashMap[Int, Int], mapCenterInfo: Map[Int, Array[Int]]) = {
    //
    //        val currPart = mapGlobalIndex.get(currK).get
    //        val lstNearest = mapCenterInfo.get(currK).get
    //
    //        var i = 0
    //        while (i < lstNearest.size && mapGlobalIndex.get(lstNearest(i)).get == currPart) i += 1
    //        if (i == lstNearest.size)
    //            currPart + 1
    //        else
    //            lstNearest(i)
    //    }

    //    private def genKeyGeom(row: (String, (String, String)), isSet1: Boolean, arrKMModelCenters: Array[(Double, Double)], mapGlobalIndex: HashMap[Int, Int]) = {
    //
    //        //        if (row._1.equalsIgnoreCase("rb_463922") ||
    //        //            row._1.equalsIgnoreCase("Ra_996669"))
    //        //            println()
    //
    //        val pointXY = (row._2._1.toDouble, row._2._2.toDouble)
    //
    //        val xy = (pointXY._1 /*% boxWidth*/ , pointXY._2 /*% boxWidth*/ )
    //
    //        //        val k = kmModel.predict(Vectors.dense(xy._1, xy._2))
    //        val k = closestCenter(xy, arrKMModelCenters)
    //
    //        val gmGeom: GMGeomBase = new GMPoint(row._1, (pointXY._1, pointXY._2))
    //
    //        val keyBase: KeyBase = if (isSet1) Key0(k) else Key1(k)
    //
    //        (keyBase, gmGeom)
    //    }

    //    def computeCenter(mbr: (Double, Double, Double, Double)) =
    //        (mbr._1 + (mbr._3 - mbr._1) / 2, mbr._2 + (mbr._4 - mbr._2) / 2)

    //    def closestCenter(pointCoords: (Double, Double), arrKMModelCenters: Array[(Double, Double)]) =
    //        (0 until arrKMModelCenters.length).map(i => (i, Helper.squaredDist(pointCoords, arrKMModelCenters(i))))
    //            .minBy(_._2)
    //            ._1

    //    def computeQuadCounts(lstAddedPoints: ListBuffer[(String, String)], fromIdx: Int, toIdx: Int, mbr: MBR) = {
    //
    //        val midX = (mbr.maxX - mbr.minX) / 2
    //        val midY = (mbr.maxY - mbr.minY) / 2
    //
    //        val (ll, lr, ur, ul) = (fromIdx to toIdx).map(i => {
    //
    //            val point = lstAddedPoints(i)
    //
    //            var (ll, lr, ur, ul) = (0L, 0L, 0L, 0L)
    //
    //            if (point._1.toDouble <= midX)
    //                if (point._2.toDouble <= midY)
    //                    ll = 1L
    //                else
    //                    ul = 1L
    //            else if (point._2.toDouble <= midY)
    //                lr = 1L
    //            else
    //                ur = 1L
    //
    //            (ll, lr, ur, ul)
    //        })
    //            .fold((0L, 0L, 0L, 0L))((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
    //
    //        SubRegionCounts(ll, lr, ur, ul)
    //    }

    //    def getValidMBR(mbr: ((String, String), (String, String))) = {
    //        if (mbr._2 == null)
    //            (mbr._1, mbr._1)
    //        else
    //            mbr
    //    }
}