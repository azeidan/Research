package org.cusp.bdi.sknn.util

import scala.collection.AbstractIterator
import scala.collection.immutable.Iterable

//object SSTest {
//
//    def main(args: Array[String]): Unit = {
//        val ss = SortSetObj[MatchedPointInfo](10)
//
//        ss.add(1, MatchedPointInfo("A", (0, 0), 1))
//        ss.add(5, MatchedPointInfo("B", (0, 0), 1))
//        ss.add(4, MatchedPointInfo("C", (0, 0), 1))
//        ss.add(1, MatchedPointInfo("D", (0, 0), 1))
//        ss.add(6, MatchedPointInfo("E", (0, 0), 1))
//        ss.add(7, MatchedPointInfo("F", (0, 0), 1))
//        ss.add(8, MatchedPointInfo("G", (0, 0), 1))
//        ss.add(-2, MatchedPointInfo("H", (0, 0), 1))
//        ss.add(3, MatchedPointInfo("I", (0, 0), 1))
//        ss.add(-4, MatchedPointInfo("J", (0, 0), 1))
//        ss.add(1000, MatchedPointInfo("K", (0, 0), 1))
//        ss.add(5, MatchedPointInfo("L", (0, 0), 1))
//        ss.add(1000, MatchedPointInfo("M", (0, 0), 1))
//        ss.add(1, MatchedPointInfo("N", (0, 0), 1))
//        ss.add(-2, MatchedPointInfo("O", (0, 0), 1))
//        ss.add(3, MatchedPointInfo("P", (0, 0), 1))
//        ss.add(-4, MatchedPointInfo("Q", (0, 0), 1))
//        ss.add(5, MatchedPointInfo("R", (0, 0), 1))
//        ss.add(1000, MatchedPointInfo("S", (0, 0), 1))
//        ss.add(1, MatchedPointInfo("T", (0, 0), 1))
//        ss.add(-2, MatchedPointInfo("U", (0, 0), 1))
//        ss.add(3, MatchedPointInfo("V", (0, 0), 1))
//        ss.add(-4, MatchedPointInfo("W", (0, 0), 1))
//        ss.add(-5, MatchedPointInfo("X", (0, 0), 1))
//        ss.add(0, MatchedPointInfo("Y", (0, 0), 1))
//        ss.add(-7, MatchedPointInfo("Z", (0, 0), 1))
//        ss.add(6, MatchedPointInfo("AA", (0, 0), 1))
//        ss.add(-6, MatchedPointInfo("Z", (0, 0), 1))
//        ss.add(606, MatchedPointInfo("AC", (0, 0), 1))
//
//        println(ss)
//    }
//}

//trait NodeData extends Comparable[NodeData] {
//
//    override def toString(): String
//}

//line: String, xy: (Double, Double)

//type T <: Comparable[T]

case class Node[T](distance: Double, data: T)(implicit ev$1: T => Comparable[_ >: T]) {

  var next: Node[T] = _

  override def toString: String =
    "%f, %s".format(distance, data)
}

case class SortedList[T](maxSize: Int, allowDuplicates: Boolean)(implicit ev$1: T => Comparable[_ >: T]) extends Serializable with Iterable[Node[T]] {

  private var headNode: Node[T] = _
  private var lastNode: Node[T] = _
  private var nodeCount = 0

  def clear(): Unit = {

    headNode = null
    lastNode = null
    nodeCount = 0
  }

  // line: String, xy: (Double, Double)
  def add(distance: Double, data: T) {

    if (!isFull || distance <= last().distance) {

      var prevNode: Node[T] = null
      var currNode = headNode

      // distance sort
      while (currNode != null && distance > currNode.distance) {

        prevNode = currNode
        currNode = currNode.next
      }

      if (allowDuplicates || currNode == null || !currNode.data.equals(data)) {

        nodeCount += 1

        if (prevNode == null) { // insert first

          headNode = Node(distance, data)
          headNode.next = currNode

          if (lastNode == null)
            lastNode = headNode
        }
        else { // insert after

          // line, xy
          prevNode.next = Node(distance, data)
          prevNode.next.next = currNode

          if (lastNode == prevNode)
            lastNode = prevNode.next
        }

        if (nodeCount > maxSize) {

          var tmp = if (prevNode == null) headNode else prevNode

          while (tmp.next != lastNode)
            tmp = tmp.next

          lastNode = tmp
          lastNode.next = null
          nodeCount -= 1
        }
      }
    }
  }

  override def last(): Node[T] = lastNode

  def isFull: Boolean = nodeCount == maxSize

  def discardAfter(node: Node[T], newCount: Int): Unit = {

    if (node.next != null) {

      lastNode = node
      lastNode.next = null

      nodeCount = newCount
    }

    //    if (!isEmpty && discardFromIndx < size)
    //      if (discardFromIndx == 0) {
    //
    //        headNode = null
    //        lastNode = headNode
    //        nodeCount = 0
    //      }
    //      else {
    //
    //        lastNode = head()
    //
    //        var idx = 0
    //
    //        while (lastNode.next != null && idx < discardFromIndx - 1) {
    //
    //          lastNode = lastNode.next
    //
    //          idx += 1
    //        }
    //
    //        //                if (currNode != null) {
    //
    //        lastNode.next = null
    //        nodeCount = idx + 1
    //        //                }
    //      }
  }

  override def size(): Int = nodeCount

  def get(idx: Int) = {

    var current = head()
    var i = 0
    while (i < idx) {

      current = current.next
      i += 1
    }

    current
  }

  override def head(): Node[T] = headNode

  override def iterator(): Iterator[Node[T]] = new AbstractIterator[Node[T]] {

    var cursor: Node[T] = if (SortedList.this.isEmpty()) null else headNode

    override def hasNext: Boolean = cursor != null

    override def next(): Node[T] =
      if (!hasNext)
        throw new NoSuchElementException("next on empty Iterator")
      else {
        val ans = cursor
        cursor = cursor.next
        ans
      }
  }

  override def isEmpty(): Boolean = headNode == null

  override def toString(): String =
    mkString("\n")

  override def mkString(sep: String): String =
    this.map(node => "%s%s".format(node, sep)).mkString("")
}