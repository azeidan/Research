package org.cusp.bdi.sknn.util

import org.cusp.bdi.ds.qt.QuadTree
import org.cusp.bdi.ds.{Box, qt}

class QuadTreeInfo {

  var uniqueIdentifier: Int = -9999

  var quadTree: QuadTree = _

  def this(boundary: Box) {

    this

    quadTree = qt.QuadTree(boundary)
  }
  override def toString: String =
    "\t%d\t%s".format(uniqueIdentifier, quadTree.toString())
}