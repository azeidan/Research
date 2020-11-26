package org.cusp.bdi.ds.bt

final class AVLNode[T] extends Serializable with Ordered[AVLNode[T]] {

  var treeHeight: Int = 1
  var nodeValue: Int = -1
  var data: T = _
  var left: AVLNode[T] = _
  var right: AVLNode[T] = _

  def this(nodeValue: Int) = {

    this()
    this.nodeValue = nodeValue
  }

  override def toString: String =
    "%s\t%s %s".format(nodeValue.toString, if (left == null) '-' else '/', if (right == null) '-' else '\\')

  override def compare(other: AVLNode[T]): Int =
    this.nodeValue.compare(other.nodeValue)
}