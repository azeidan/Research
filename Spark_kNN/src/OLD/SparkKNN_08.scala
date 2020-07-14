//package org.cusp.bdi.sknn
//
//import scala.collection.mutable.HashMap
//import scala.collection.mutable.ListBuffer
//import scala.collection.mutable.Map
//import scala.collection.mutable.SortedSet
//
//import org.apache.hadoop.io.compress.GzipCodec
//import org.apache.spark.Partitioner
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//import org.apache.spark.mllib.clustering.DistanceMeasure
//import org.apache.spark.mllib.clustering.KMeans
//import org.apache.spark.mllib.linalg.Vector
//import org.apache.spark.mllib.linalg.Vectors
//import org.apache.spark.serializer.KryoSerializer
//import org.cusp.bdi.gm.GeoMatch
//import org.cusp.bdi.gm.geom.GMGeomBase
//import org.cusp.bdi.gm.geom.GMPoint
//import org.cusp.bdi.sknn.util.RDD_Store
//import org.cusp.bdi.sknn.util.STRtreeOperations
//import org.cusp.bdi.sknn.util.SparkKNN_Arguments
//import org.cusp.bdi.sknn.util.SparkKNN_Local_CLArgs
//import org.cusp.bdi.util.Helper
//import org.locationtech.jts.geom.GeometryFactory
//import org.locationtech.jts.geom.Point
//import org.locationtech.jts.index.strtree.AbstractNode
//import org.locationtech.jts.index.strtree.ItemBoundable
//import org.locationtech.jts.index.strtree.ItemDistance
//import org.locationtech.jts.index.strtree.STRtree
//import org.apache.spark.mllib.clustering.KMeansModel
//import org.locationtech.jts.geom.Coordinate
//import scala.collection.mutable.Set
//import scala.collection.mutable.HashSet
//import org.locationtech.jts.geom.Geometry
//import org.locationtech.jts.util.GeometricShapeFactory
//import org.locationtech.jts.geom.Envelope
//import org.apache.spark.HashPartitioner
//
//object SparkKNN {
//
//    def main(args: Array[String]): Unit = {
//
//        val startTime = System.currentTimeMillis()
//        var startTime2 = startTime
//
//        val clArgs = SparkKNN_Local_CLArgs.randomPoints_randomPoints
//        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_busPointShift
//        //        val clArgs = SparkKNN_Local_CLArgs.busPoint_taxiPoint
//        //        val clArgs = CLArgsParser(args, SparkKNN_Arguments())
//
//        val sparkConf = new SparkConf()
//            .setAppName(this.getClass.getName)
//            .set("spark.serializer", classOf[KryoSerializer].getName)
//            .registerKryoClasses(GeoMatch.getGeoMatchClasses())
//            .registerKryoClasses(Array(classOf[KeyBase]))
//
//        if (clArgs.getParamValueBoolean(SparkKNN_Arguments.local))
//            sparkConf.setMaster("local[*]")
//
//        val sc = new SparkContext(sparkConf)
//
//        val kParam = clArgs.getParamValueInt(SparkKNN_Arguments.k)
//        //        val numIter = clArgs.getParamValueInt(SparkKNN_Arguments.numIter)
//        val minPartitions = clArgs.getParamValueInt(SparkKNN_Arguments.minPartitions)
//        val outDir = clArgs.getParamValueString(SparkKNN_Arguments.outDir)
//
//        // delete output dir if exists
//        Helper.delDirHDFS(sc, clArgs.getParamValueString(SparkKNN_Arguments.outDir))
//
//        val rddDS1Plain = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.firstSet), minPartitions)
//            .mapPartitions(_.map(RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.firstSetObj))))
//        //            .reduceByKey((x, y) => x) // get rid of duplicates
//        //            .persist()
//
//        val numParts = rddDS1Plain.getNumPartitions
//        val rowsPerPartition = (sc.runJob(rddDS1Plain, getIteratorSize _, Array(0, numParts / 2, numParts - 1)).sum / 3)
//
//        var rddDS1_XY = rddDS1Plain
//            .mapPartitions(_.map(row => (row._2._1, row._2._2)))
//            //            .reduceByKey(_ + _) // eliminate duplicates
//            .repartitionAndSortWithinPartitions(new HashPartitioner(numParts)) // group along the x-axis
//            .persist()
//
//        val kmeansK = rddDS1_XY.getNumPartitions
//
//        var kmeans = new KMeans().setK(kmeansK).setSeed(1L).setDistanceMeasure(DistanceMeasure.EUCLIDEAN)
//        var kmModel = kmeans.run(rddDS1_XY.mapPartitions(_.map(row => Vectors.dense(row._1.toDouble, row._2.toDouble))))
//
//        //        val kmModelCenters = new Array[(Int, (Double, Double))](kmModel.clusterCenters.length)
//        //        val arrKMModelCenters = (0 until kmModel.clusterCenters.length)
//        //            .map(i => (i, kmModel.clusterCenters(i)))
//        kmeans = null
//        //        kmModel = null
//
//        println(">T>Kmeans: %f".format((System.currentTimeMillis() - startTime2) / 1000.0))
//
//        startTime2 = System.currentTimeMillis()
//
//        var mapKtoPart = (0 until kmModel.clusterCenters.length).map(i => (i, i)).toMap
//
//        var mapGlobalIndex = rddDS1_XY.mapPartitions(_.map(row => {
//
//            val partNum = mapKtoPart.get(kmModel.predict(Vectors.dense(row._1.toDouble, row._2.toDouble))).get
//
//            (partNum, ListBuffer(row))
//        }))
//            .reduceByKey((lst1, lst2) => {
//
//                lst2.foreach(point => lst1.append(point))
//
//                lst1
//            })
//            .mapPartitions(iter => {
//
//                var partK = -1
//                var lstPoints: ListBuffer[(Double, Double)] = null
//                var (minX, minY, maxX, maxY) = (0.0, 0.0, 0.0, 0.0)
//
//                while (iter.hasNext) {
//
//                    val row = iter.next
//                    lstPoints = row._2.map(x => (x._1.toDouble, x._2.toDouble))
//
//                    if (partK == -1) partK = row._1
//                    else if (partK != row._1)
//                        throw new Exception("Found more than one K on the same partition. Should not happen at this point. (%d and %d)".format(partK, row._1))
//
//                    // compute MBR
//                    val (x1: Double, y1: Double, x2: Double, y2: Double) = lstPoints.fold((Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))((x, y) => {
//
//                        val (a, b, c, d) = x.asInstanceOf[(Double, Double, Double, Double)]
//                        val t = y.asInstanceOf[(Double, Double)]
//
//                        (math.min(a, t._1), math.min(b, t._2), math.max(c, t._1), math.max(d, t._2))
//                    })
//
//                    minX = x1
//                    minY = y1
//                    maxX = x2
//                    maxY = y2
//                }
//
//                // (k, MBR, (LL_Count, LR_Count, UR_Count, UL_Count)
//                val midX = (maxX - minX) / 2
//                val midY = (maxY - minY) / 2
//                val quadCounts = lstPoints.map(point => {
//
//                    var (ll, lr, ur, ul) = (0, 0, 0, 0)
//
//                    if (point._1 <= midX)
//                        if (point._2 <= midY)
//                            ll = 1
//                        else
//                            ul = 1
//                    else if (point._2 <= midY)
//                        lr = 1
//                    else
//                        ur = 1
//
//                    (ll, lr, ur, ul)
//                })
//                    .fold((0, 0, 0, 0))((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
//                Iterator((partK, ((minX, minY, maxX, maxY), quadCounts)))
//            })
//            .collect()
//            .toMap
//
//        //        mapGlobalIndex.foreach(x => println(">>\t%s".format(x).replaceAll(",", "\\\\t").replaceAll("\\(", "").replaceAll("\\)", "")))
//
//        val mapClosest = HashMap[Int, Array[Int]]() ++ (0 until mapGlobalIndex.size).map(key1 => {
//
//            val row1 = mapGlobalIndex.get(key1).get
//
//            val arrClosestK = (0 until mapGlobalIndex.size).map(key2 => {
//
//                val row2 = mapGlobalIndex.get(key2).get
//
//                if (key1.equals(key2))
//                    null
//                else
//                    (Helper.squaredDist((row1._1._3 - row1._1._1, row1._1._4 - row1._1._2), (row2._1._3 - row2._1._1, row2._1._4 - row2._1._2)), key2)
//            })
//                .filter(_ != null)
//                .to[SortedSet]
//                .toArray
//                .map(_._2)
//
//            (key1, arrClosestK)
//        })
//
//        //        mapClosest.foreach(x => println(">>\t%d\t%s".format(x._1, x._2.mkString("\t"))))
//
//        println(">T>mapGlobalIndex: %f".format((System.currentTimeMillis() - startTime2) / 1000.0))
//
//        rddDS1_XY.unpersist(false)
//        rddDS1_XY = null
//
//        startTime2 = System.currentTimeMillis()
//
//        val partitionerByK = new Partitioner() {
//
//            override def numPartitions = rddDS1Plain.getNumPartitions
//            override def getPartition(key: Any): Int =
//                key match {
//                    case keyBase: KeyBase => keyBase.k
//                }
//        }
//
//        val rddDS1 = rddDS1Plain
//            .mapPartitionsWithIndex((pIdx, iter) => iter.map(row => {
//
//                val key: KeyBase = Key0(kmModel.predict(Vectors.dense(row._2._1.toDouble, row._2._2.toDouble)))
//
//                (key, row)
//            }))
//            .partitionBy(partitionerByK)
//            .mapPartitionsWithIndex((pIdx, iter) => {
//
//                val sTRtree = new STRtree
//                val jtsGeomFact = new GeometryFactory
//
//                var key0: KeyBase = null
//                var row: (KeyBase, (String, (String, String))) = null
//
//                while (iter.hasNext) {
//
//                    row = iter.next
//
//                    val pointXY = (row._2._2._1.toDouble, row._2._2._2.toDouble)
//
//                    val gmGeom: GMGeomBase = new GMPoint(row._2._1, (pointXY._1, pointXY._2))
//                    //                    if (gmGeom.payload.equalsIgnoreCase("Ra_401973"))
//                    //                        println
//                    val env = gmGeom.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//
//                    sTRtree.insert(env, (gmGeom, SortSetObj(kParam)))
//                }
//
//                val ret: (KeyBase, (STRtree, GMGeomBase, SortSetObj, ListBuffer[Int])) = (row._1, (sTRtree, null, null, null))
//
//                Iterator(ret)
//            }, true)
//            .partitionBy(partitionerByK)
//
//        val rddDS2 = RDD_Store.getRDDPlain(sc, clArgs.getParamValueString(SparkKNN_Arguments.secondSet), minPartitions)
//            .mapPartitionsWithIndex((pIdx, iter) => {
//
//                val lineParser = RDD_Store.getLineParser(clArgs.getParamValueString(SparkKNN_Arguments.secondSetObj))
//
//                val jtsGeomFact = new GeometryFactory
//
//                iter.map(line => {
//
//                    val row = lineParser(line)
//
//                    val pointXY = (row._2._1.toDouble, row._2._2.toDouble)
//
//                    var k = kmModel.predict(Vectors.dense(pointXY._1, pointXY._2))
//
//                    //                    if (row._1.equalsIgnoreCase("Rb_407608"))
//                    //                        println
//
//                    val gmGeom: GMGeomBase = new GMPoint(row._1, (pointXY._1, pointXY._2))
//                    //                    if (gmGeom.payload.equalsIgnoreCase("Rb_978146"))
//                    //                        println
//                    // list  of nearby kmeans' ks that sum up to at least the needed k
//                    var pointsInRadius = 0
//                    var kInfo = mapGlobalIndex.get(k).get
//                    //                    val center = computeCenter(kInfo._1)
//
//                    var radius = math.max(kInfo._1._3 - kInfo._1._1, kInfo._1._4 - kInfo._1._2)
//                    //                    val setCheckedParts = HashSet[Int]()
//
//                    //                    if (gmGeom.payload.equalsIgnoreCase("Rb_278422"))
//                    //                        println
//
//                    var loopCounter = 0
//                    var setClosest = SortedSet[(Int, Int)]()
//
//                    val jtsEnv = gmGeom.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//
//                    while (pointsInRadius < kParam) {
//
//                        jtsEnv.expandBy(radius)
//                        val geom = jtsGeomFact.toGeometry(jtsEnv)
//
//                        // intersects with
//                        mapGlobalIndex.foreach(kInfo2 => {
//
//                            if (!setClosest.map(_._2).contains(kInfo2._1)) {
//
//                                val centerK = computeCenter(kInfo2._2._1)
//                                val (minX, minY, maxX, maxY) = kInfo2._2._1
//
//                                val arr = Array((jtsGeomFact.toGeometry(new Envelope(minX, centerK._1, minY, centerK._2)), kInfo2._2._2._1),
//                                    (jtsGeomFact.toGeometry(new Envelope(centerK._1, maxX, minY, centerK._2)), kInfo2._2._2._2),
//                                    (jtsGeomFact.toGeometry(new Envelope(centerK._1, maxX, centerK._2, maxY)), kInfo2._2._2._3),
//                                    (jtsGeomFact.toGeometry(new Envelope(minX, centerK._1, centerK._2, maxY)), kInfo2._2._2._4))
//
//                                val pointCount = arr
//                                    .map(subRegion => {
//
//                                        val b = geom.intersects(subRegion._1)
//                                        val c = geom.contains(subRegion._1)
//
//                                        if (geom.intersects(subRegion._1)) subRegion._2 else 0
//                                    })
//                                    .sum
//
//                                if (pointCount > 0) {
//
//                                    //                                    setCheckedParts.add(kInfo2._1)
//                                    setClosest.add((loopCounter, kInfo2._1))
//                                    pointsInRadius += pointCount
//                                }
//                            }
//                        })
//
//                        loopCounter += 1
//                    }
//
//                    val lstPartToVisit = setClosest.to[ListBuffer].map(_._2)
//
//                    k = lstPartToVisit(0)
//                    lstPartToVisit.remove(0)
//
//                    val ret: (KeyBase, (STRtree, GMGeomBase, SortSetObj, ListBuffer[Int])) = (Key1(k), (null, gmGeom, SortSetObj(kParam), lstPartToVisit))
//
//                    println(">>%s\t%s\t%s".format(ret._1, ret._2._2.payload, ret._2._4.mkString("\t")))
//
//                    ret
//                })
//            })
//            .partitionBy(partitionerByK)
//
//        var rddResult = rddDS1.union(rddDS2)
//
//        (0 until 9).foreach(i => {
//
//            if (i > 0)
//                rddResult = rddResult.repartitionAndSortWithinPartitions(partitionerByK)
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
//            .mapPartitions(_.map(row => "%s,%.8f,%.8f%s".format(row._1.payload, row._1.coordArr(0)._1, row._1.coordArr(0)._2, row._2.toString())))
//            .saveAsTextFile(outDir, classOf[GzipCodec])
//
//        //        val itemDist = new ItemDistance() with Serializable {
//        //            override def distance(item1: ItemBoundable, item2: ItemBoundable) = {
//        //
//        //                val jtsGeomFact = new GeometryFactory
//        //                item1.getItem.asInstanceOf[(GMGeomBase, SortSetObj)]._1.toJTS(jtsGeomFact)(0).distance(
//        //                    item2.getItem.asInstanceOf[Point])
//        //            }
//        //        }
//        //
//        //        var rdd = rddDS1.union(rddDS2)
//        //            .mapPartitionsWithIndex((pIdx, iter) => {
//        //
//        //                val sTRtree = new STRtree
//        //                val jtsGeomFact = new GeometryFactory
//        //                var newKey1: KeyBase = null
//        //                var sTRtreeKey: KeyBase = null
//        //
//        //                iter.map(row => {
//        //
//        //                    row._1 match {
//        //                        case _: Key0 => {
//        //
//        //                            val env = row._2.toJTS(jtsGeomFact)(0).getEnvelopeInternal
//        //
//        //                            sTRtree.insert(env, (row._2, SortSetObj(kParam)))
//        //
//        //                            if (sTRtreeKey == null)
//        //                                sTRtreeKey = row._1
//        //
//        //                            if (iter.hasNext)
//        //                                null
//        //                            else
//        //                                Iterator((row._1, sTRtree))
//        //                        }
//        //                        case _: Key1 => {
//        //
//        //                            //                            if (row._2.payload.equalsIgnoreCase("B200463") ||
//        //                            //                                row._2.payload.equalsIgnoreCase("B200321") ||
//        //                            //                                row._2.payload.equalsIgnoreCase("B200066"))
//        //                            //                                println()
//        //
//        //                            val (gmGeom, gmGeomSet) = (row._2, SortSetObj(kParam))
//        //
//        //                            if (gmGeom.payload.equalsIgnoreCase("rb_463922"))
//        //                                println
//        //
//        //                            if (sTRtree != null)
//        //                                STRtreeOperations.rTreeNearestNeighbor(jtsGeomFact, gmGeom, gmGeomSet, kParam * 5, sTRtree) // *5 for boundery objects
//        //
//        //                            val newKey1 = Key1(getNextPart(row._1.k, mapGlobalIndex, mapCenterInfo))
//        //                            val ds2Row: (KeyBase, Any) = (newKey1, (gmGeom, gmGeomSet))
//        //
//        //                            if (iter.hasNext)
//        //                                Iterator(ds2Row)
//        //                            else
//        //                                Iterator(ds2Row, (sTRtreeKey, sTRtree))
//        //                        }
//        //                    }
//        //                })
//        //                    .filter(_ != null)
//        //                    .flatMap(_.seq)
//        //            }, true)
//        //
//        //        // 2nd round
//        //        val rddResult = rdd.repartitionAndSortWithinPartitions(partitionerByK)
//        //            .mapPartitions(iter => {
//        //
//        //                var sTRtree: STRtree = null
//        //                var rTreeKey: KeyBase = null
//        //                val jtsGeomFact = new GeometryFactory
//        //
//        //                iter.map(row => {
//        //
//        //                    row._1 match {
//        //                        case _: Key0 => {
//        //
//        //                            sTRtree = row._2 match { case rt: STRtree => rt }
//        //                            rTreeKey = row._1
//        //
//        //                            if (iter.hasNext)
//        //                                null
//        //                            else
//        //                                Iterator[Any](sTRtree)
//        //                        }
//        //                        case _: Key1 => {
//        //
//        //                            val (gmGeom, gmGeomSet) = row._2.asInstanceOf[(GMGeomBase, SortSetObj)]
//        //
//        //                            if (sTRtree != null)
//        //                                STRtreeOperations.rTreeNearestNeighbor(jtsGeomFact, gmGeom, gmGeomSet, kParam, sTRtree)
//        //
//        //                            val ds2Row = (gmGeom, gmGeomSet)
//        //
//        //                            if (iter.hasNext)
//        //                                Iterator(ds2Row)
//        //                            else
//        //                                Iterator[Any](sTRtree, ds2Row)
//        //                        }
//        //                    }
//        //                })
//        //
//        //                    .filter(_ != null)
//        //                    .flatMap(_.seq)
//        //            }, true)
//        //
//        //        rddResult.mapPartitions(_.map(row => {
//        //
//        //            row match {
//        //                case sTRtree: STRtree => {
//        //
//        //                    val lst = ListBuffer[(GMGeomBase, SortSetObj)]()
//        //
//        //                    getTreeItems(sTRtree.getRoot, lst)
//        //
//        //                    lst.iterator
//        //                }
//        //                case _ => Iterator(row.asInstanceOf[(GMGeomBase, SortSetObj)])
//        //            }
//        //        })
//        //            .flatMap(_.seq))
//        //            .mapPartitions(_.map(row => "%s,%.8f,%.8f%s".format(row._1.payload, row._1.coordArr(0)._1, row._1.coordArr(0)._2, row._2.toString())))
//        //            .saveAsTextFile(outDir, classOf[GzipCodec])
//
//        printf("Total Time: %,.2f Sec%n", (System.currentTimeMillis() - startTime) / 1000.0)
//
//        println(outDir)
//    }
//
//    import scala.collection.JavaConversions._
//    private def getTreeItems(node: AbstractNode, lst: ListBuffer[(GMGeomBase, SortSetObj)]) {
//
//        node.getChildBoundables.foreach(item => {
//
//            item match {
//                case an: AbstractNode => getTreeItems(an, lst)
//                case ib: ItemBoundable => lst.append(ib.getItem().asInstanceOf[(GMGeomBase, SortSetObj)])
//            }
//        })
//    }
//
//    //    private def euclideanDist(vect1: Vector, vect2: Vector) = {
//    //
//    //        val arr1 = vect1.toArray
//    //        val arr2 = vect2.toArray
//    //
//    //        Helper.euclideanDist((arr1(0), arr1(1)), (arr2(0), arr2(1)))
//    //    }
//
//    private def getIteratorSize(iter: Iterator[_]) = {
//        var count = 0
//        var size = 0
//        while (iter.hasNext) {
//            count += 1
//            iter.next()
//        }
//        count
//    }
//
//    //    def computeMBR(sc: SparkContext, fileName: String, datasetObj: String, minPartitions: Int) = {
//    //        RDD_Store.getRDDPlain(sc, fileName, minPartitions)
//    //            .mapPartitionsWithIndex((pIdx, iter) => {
//    //
//    //                val lineParser = RDD_Store.getLineParser(datasetObj)
//    //
//    //                var (minX: Long, minY: Long, maxX: Long, maxY: Long) =
//    //                    iter.map(lineParser)
//    //                        .map(row => (row._2._1.toDouble, row._2._2.toDouble))
//    //                        .fold((Long.MaxValue, Long.MaxValue, Long.MinValue, Long.MinValue))((x, y) => {
//    //
//    //                            val (a, b, c, d) = x.asInstanceOf[(Long, Long, Long, Long)]
//    //                            val t = y.asInstanceOf[(Long, Long)]
//    //
//    //                            (math.min(a, t._1), math.min(b, t._2), math.max(c, t._1), math.max(d, t._2))
//    //                        })
//    //                Iterator((minX, minY, maxX, maxY))
//    //            })
//    //            .fold((Long.MaxValue, Long.MaxValue, Long.MinValue, Long.MinValue))((x, y) => {
//    //
//    //                val (a, b, c, d) = x.asInstanceOf[(Long, Long, Long, Long)]
//    //                val t = y.asInstanceOf[(Long, Long, Long, Long)]
//    //
//    //                (math.min(a, t._1), math.min(b, t._2), math.max(c, t._3), math.max(d, t._4))
//    //            })
//    //    }
//
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
//
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
//
//    def computeCenter(mbr: (Double, Double, Double, Double)) =
//        (mbr._1 + (mbr._3 - mbr._1) / 2, mbr._2 + (mbr._4 - mbr._2) / 2)
//
//    def closestCenter(pointCoords: (Double, Double), arrKMModelCenters: Array[(Double, Double)]) =
//        (0 until arrKMModelCenters.length).map(i => (i, Helper.squaredDist(pointCoords, arrKMModelCenters(i))))
//            .minBy(_._2)
//            ._1
//}