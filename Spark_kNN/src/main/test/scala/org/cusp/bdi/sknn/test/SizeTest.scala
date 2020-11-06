package org.cusp.bdi.sknn.test

import org.apache.spark.util.SizeEstimator
import org.cusp.bdi.ds.SpatialIndex.buildRectBounds
import org.cusp.bdi.ds.geom.Point
import org.cusp.bdi.ds.kdt.KdtBranchRootNode
import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.util.{InputFileParsers, LocalRunConsts}

import scala.io.Source

object SizeTest {

  def main(args: Array[String]): Unit = {
//    val lstInfo = Source.fromFile(LocalRunConsts.pathRandSample_A_NAD83)
//      .getLines()
//      .map(InputFileParsers.threePartLine)
//      .filter(_ != null)
//      .map(row => (row._2._1.toDouble, row._2._2.toDouble, row._1))
//      .toList
//
//    println("lstInfo.size: " + lstInfo.size)
//
//    val mbr = lstInfo.map(row => (row._1, row._2, row._1, row._2))
//      .fold((Double.MaxValue, Double.MaxValue, Double.MinValue, Double.MinValue))((mbr1, mbr2) =>
//        (math.min(mbr1._1, mbr2._1), math.min(mbr1._2, mbr2._2), math.max(mbr1._3, mbr2._3), math.max(mbr1._4, mbr2._4)))
//
//    println("QT Empty Size: " + SizeEstimator.estimate(new QuadTree(null)))
//    //    println("KDTNode Empty Size: " + SizeEstimator.estimate(new KdtLeafNode()))
//    println("KDTBranchRootNode Empty Size: " + SizeEstimator.estimate(new KdtBranchRootNode()))
//    //    println("KDT Empty Size: " + SizeEstimator.estimate(new KdTree(1)))
//
//    val lstPoints = lstInfo.map(row => new Point(row._1, row._2, row._3))
//
//    var start = System.currentTimeMillis()
//
//    val qt = new QuadTree(buildRectBounds((mbr._1.floor, mbr._2.floor), (mbr._3.ceil, mbr._4.ceil)))
//    qt.insert(lstPoints.toIterator)
//    printf("QT size: %,d in %,d%n".format(SizeEstimator.estimate(qt), System.currentTimeMillis() - start))
//
//    start = System.currentTimeMillis()
//
//    //    val kdt = new KdTree(6)
//    //    kdt.insert(lstPoints.toIterator)
//    //    printf("KdT size: %,d in %,d%n".format(SizeEstimator.estimate(kdt), System.currentTimeMillis() - start))
//
//    start = System.currentTimeMillis()
//
//    //    val kdt2 = new KdTree(1)
//    //    kdt2.insert(lstPoints.toIterator)
//    //    printf("KdT2 size: %,d in %,d%n".format(SizeEstimator.estimate(kdt2), System.currentTimeMillis() - start))
  }
}
