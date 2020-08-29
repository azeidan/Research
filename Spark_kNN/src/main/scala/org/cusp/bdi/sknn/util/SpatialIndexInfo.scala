//package org.cusp.bdi.sknn.util
//
//import org.cusp.bdi.ds.Box
//import org.cusp.bdi.ds.qt.QuadTree
//
//class SpatialIndexInfo {
//
//  var uniqueIdentifier: Int = -9999
//
//  var quadTree: QuadTree = _
//
//  def this(boundary: Box) {
//
//    this
//
//    quadTree = QuadTree(boundary)
//  }
//  override def toString: String =
//    "\t%d\t%s".format(uniqueIdentifier, quadTree.toString())
//}