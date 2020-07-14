//package org.cusp.bdi.sknn
//
//import org.apache.spark.rdd.RDD
//import org.cusp.bdi.gm.geom.GMGeomBase
//import org.cusp.bdi.sknn.util.STRtreeOperations
//import org.locationtech.jts.index.strtree.STRtree
//
//object ResultRDDtoString {
//    def apply(rdd: RDD[(KeyBase, Any)]) = {
//
//        rdd.mapPartitions(_.map(row => {
//
//            row._1 match {
//                case _: Key0 =>
//                    STRtreeOperations.getTreeItems(row._2 match { case rt: STRtree => rt })
//                        .iterator
//                case _ => Iterator(row._2.asInstanceOf[(GMGeomBase, SortSetObj)])
//            }
//        })
//            .flatMap(_.seq))
//            .mapPartitions(_.map(row => {
//
//                StringBuilder.newBuilder
//                    .append(row._1.payload)
//                    .append(row._2.toString())
//                    .toString()
//            }))
//    }
//}