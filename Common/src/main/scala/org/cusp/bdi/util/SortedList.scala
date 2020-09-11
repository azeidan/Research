package org.cusp.bdi.util

import scala.collection.AbstractIterator
import scala.collection.immutable.Iterable

case class Node[T](distance: Double, data: T) /*(implicit ev$1: T => Comparable[_ >: T])*/ {

  var next: Node[T] = _

  override def toString: String =
    "%f, %s".format(distance, data)
}

case class SortedList[T](maxSize: Int) /*(implicit ev$1: T => Comparable[_ >: T])*/ extends Serializable with Iterable[Node[T]] {

  private var headNode: Node[T] = _
  private var lastNode: Node[T] = _
  private var nodeCount = 0

  def clear(): Unit = {

    headNode = null
    lastNode = null
    nodeCount = 0
  }

  def add(distance: Double, data: T) {

    if (!isFull || distance <= last().distance) {

      var prevNode: Node[T] = null
      var currNode = headNode

      // distance sort
      while (currNode != null && distance > currNode.distance) {

        prevNode = currNode
        currNode = currNode.next
      }

      if (currNode == null || !currNode.data.equals(data)) {

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

  def stopAt(node: Node[T]): Unit = {

    if (node != null && node.next != null) {

      lastNode = node
      lastNode.next = null
    }
  }

  override def size(): Int = nodeCount

  def get(idx: Int): Node[T] = {

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