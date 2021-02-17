package org.cusp.bdi.util

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class RandomWeighted extends Serializable {

  private val random = Random
  protected var totalItemWeight = 0.0f
  protected val arrItemWeight = ArrayBuffer[(Int, Float)]()

  def add(item: Int, weight: Float): RandomWeighted = {

    totalItemWeight += weight;
    arrItemWeight += ((item, totalItemWeight))

    this
  }

  def getRandomItem(): Int = {

    val rand = random.nextDouble() * totalItemWeight;

    arrItemWeight.filter(_._2 >= rand).take(1).head._1
  }
}
