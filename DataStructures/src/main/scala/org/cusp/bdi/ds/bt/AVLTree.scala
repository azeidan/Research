package org.cusp.bdi.ds.bt

import org.cusp.bdi.ds.bt.AVLTree.{heightDiff, nodeHeight}

import scala.collection.mutable.ListBuffer
import scala.collection.{AbstractIterator, mutable}

object AVLTree {

  def apply[T]() =
    new AVLTree[T]

  def nodeHeight(node: AVLNode[_]): Int =
    if (node == null)
      0
    else
      node.treeHeight

  def max(x: Int, y: Int): Int =
    if (x > y) x else y

  def heightDiff(node: AVLNode[_]): Int =
    if (node == null)
      0
    else
      nodeHeight(node.left) - nodeHeight(node.right)
}

class AVLTree[T] extends Serializable /* with Iterable[AVLNode[T]]*/ {

  var rootNode: AVLNode[T] = _

  def lookupKeyValue(valueLookup: Int): AVLNode[T] = {

    var currNode = rootNode

    while (currNode != null)
      valueLookup.compare(currNode.keyValue).signum match {
        case -1 =>
          currNode = currNode.left
        case 0 =>
          return currNode
        case 1 =>
          currNode = currNode.right
      }

    null
  }

  def getOrElseInsert(insertKey: Int): AVLNode[T] = {

    val newNode = new AVLNode[T](insertKey)

    val res = getOrElseInsert(rootNode, newNode)

    rootNode = res._1

    res._2
  }

  def getOrElseInsert(newNode: AVLNode[T]): AVLNode[T] = {

    val res = getOrElseInsert(rootNode, newNode)

    rootNode = res._1

    if (newNode != res._2)
      throw new Exception("Duplicate Node")

    newNode
  }

  private def getOrElseInsert(currNode: AVLNode[T], newNode: AVLNode[T]): (AVLNode[T], AVLNode[T]) = {

    var valueNode = rootNode

    if (currNode == null) {

      valueNode = newNode

      return (valueNode, valueNode)
    } else {

      newNode.keyValue.compare(currNode.keyValue).signum match {
        case -1 =>

          val res = getOrElseInsert(currNode.left, newNode)
          currNode.left = res._1
          valueNode = res._2
        case 0 =>
          return (currNode, currNode)
        case 1 =>

          val res = getOrElseInsert(currNode.right, newNode)
          currNode.right = res._1
          valueNode = res._2
      }

      currNode.treeHeight = 1 + AVLTree.max(nodeHeight(currNode.left), nodeHeight(currNode.right))

      val hDiff = heightDiff(currNode)

      if (hDiff > 1) {

        val keyDiff = newNode.keyValue - currNode.left.keyValue

        if (keyDiff < 0)
          return (rotateRight(currNode), valueNode)
        else if (keyDiff > 0) {

          currNode.left = rotateLeft(currNode.left)
          return (rotateRight(currNode), valueNode)
        }
      }
      else if (hDiff < -1) {

        val keyDiff = newNode.keyValue - currNode.right.keyValue

        if (keyDiff < 0) {

          currNode.right = rotateRight(currNode.right)
          return (rotateLeft(currNode), valueNode)
        }
        else if (keyDiff > 0)
          return (rotateLeft(currNode), valueNode)
      }
    }

    (currNode, valueNode)
  }

  def allNodes: Iterator[AVLNode[T]] = new AbstractIterator[AVLNode[T]] {

    private val stack = if (rootNode == null) null else mutable.Stack(rootNode)

    override def hasNext: Boolean = stack != null && stack.nonEmpty

    override def next(): AVLNode[T] =
      if (hasNext) {

        val ret = stack.pop()

        if (ret.left != null)
          stack.push(ret.left)

        if (ret.right != null)
          stack.push(ret.right)

        ret
      }
      else
        throw new NoSuchElementException("next on empty Iterator")
  }

  def inOrder(): ListBuffer[AVLNode[T]] = {

    val retLst = ListBuffer[AVLNode[T]]()

    val stackNode = mutable.Stack[AVLNode[T]]()
    var currNode = rootNode

    while (currNode != null || stackNode.nonEmpty) {

      while (currNode != null) {

        stackNode.push(currNode)
        currNode = currNode.left
      }

      currNode = stackNode.pop
      retLst += currNode

      currNode = currNode.right
    }

    retLst
  }

  private def rotateRight(node: AVLNode[T]) = {

    val newSubtreeRoot = node.left
    val temp = newSubtreeRoot.right

    // rotate
    newSubtreeRoot.right = node
    node.left = temp

    node.treeHeight = AVLTree.max(nodeHeight(node.left), nodeHeight(node.right)) + 1
    newSubtreeRoot.treeHeight = AVLTree.max(nodeHeight(newSubtreeRoot.left), nodeHeight(newSubtreeRoot.right)) + 1

    newSubtreeRoot
  }

  private def rotateLeft(node: AVLNode[T]) = {

    val newSubtreeRoot = node.right
    val temp = newSubtreeRoot.left

    // rotate
    newSubtreeRoot.left = node
    node.right = temp

    node.treeHeight = AVLTree.max(nodeHeight(node.left), nodeHeight(node.right)) + 1
    newSubtreeRoot.treeHeight = AVLTree.max(nodeHeight(newSubtreeRoot.left), nodeHeight(newSubtreeRoot.right)) + 1

    newSubtreeRoot
  }

  override def toString: String =
    rootNode.toString
}