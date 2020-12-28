package org.cusp.bdi.ds.sortset

import scala.collection.AbstractIterator
import scala.collection.immutable.Iterable

case class Node[T](distance: Double, data: T) /*(implicit ev$1: T => Comparable[_ >: T])*/ {

  var next: Node[T] = _

  override def toString: String =
    "%f, %s".format(distance, data)
}

case class SortedLinkedList[T](maxSize: Int) /*(implicit ev$1: T => Comparable[_ >: T])*/ extends Serializable with Iterable[Node[T]] {

  private var headNode: Node[T] = _
  private var lastNode: Node[T] = _
  private var nodeCount = 0

  if (maxSize < 1)
    throw new IllegalArgumentException("Invalid maxSize parameter. Values > 0 only")

  def this() =
    this(Int.MaxValue)

  def clear(): Unit = {

    headNode = null
    lastNode = null
    nodeCount = 0
  }

  def add(distance: Double, data: T) {

    if (nodeCount < maxSize || distance <= last.distance) {

      var prevNode: Node[T] = null
      var currNode = headNode
      var prevNodeIndex = -1

      if (last != null && distance >= last.distance) {

        prevNodeIndex += nodeCount
        prevNode = last
        currNode = null
      }

      // distance sort
      if (currNode != null)
        while (distance > currNode.distance) {

          prevNodeIndex += 1

          prevNode = currNode
          currNode = currNode.next
        }

      nodeCount += 1

      if (prevNode == null) { // insert first

        headNode = Node(distance, data)
        headNode.next = currNode

        if (lastNode == null)
          lastNode = headNode
      }
      else { // insert after

        prevNode.next = Node(distance, data)
        prevNode.next.next = currNode

        if (lastNode == prevNode)
          lastNode = prevNode.next
      }

      if (nodeCount > maxSize) {

        if (prevNode == null) {

          prevNode = headNode
          prevNodeIndex = 0
        }

        // jump to maxSize node
        while ((prevNodeIndex < maxSize - 1) || (prevNode.next != null && prevNode.distance == prevNode.next.distance)) {

          prevNode = prevNode.next
          prevNodeIndex += 1
        }

        if (prevNode != lastNode) {

          lastNode = prevNode
          lastNode.next = null
          nodeCount = prevNodeIndex + 1
        }
      }
    }
  }

  def isFull: Boolean = nodeCount >= maxSize

  def stopAt(node: Node[T]): Unit = {

    if (node != null && node.next != null) {

      lastNode = node
      lastNode.next = null
    }
  }

  def get(idx: Int): Node[T] = {

    var current = head
    var i = 0
    while (i < idx) {

      current = current.next
      i += 1
    }

    current
  }

  override def head: Node[T] = headNode

  override def last: Node[T] = lastNode

  def length: Int = nodeCount

  override def size: Int = nodeCount

  override def iterator: Iterator[Node[T]] = new AbstractIterator[Node[T]] {

    var cursor: Node[T] = if (SortedLinkedList.this.isEmpty()) null else headNode

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
