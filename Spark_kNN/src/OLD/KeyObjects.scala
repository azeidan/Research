//package org.cusp.bdi.sknn
//
//abstract class KeyBase(_partId: Int) extends Serializable /* with Ordered[KeyBase]*/ {
//
//    var partId = _partId
//    //    var regionID: String = null
//
//    //    implicit def ordering[A <: KeyBase]: Ordering[A] = new Ordering[A] {
//    //        override def compare(x: A, y: A): Int = {
//    //            x.compareTo(y)
//    //        }
//    //    }
//
//    //    // Instances of Key0 always appear before Key1
//    //    override def compare(that: KeyBase): Int = {
//    //
//    //        this match {
//    //            case _: Key0 => {
//    //                that match {
//    //                    case _: Key0 => 0
//    //                    case _: Key1 => -1
//    //                }
//    //            }
//    //            case _ => {
//    //                that match {
//    //                    case _: Key0 => 1
//    //                    case _: Key1 => 0
//    //                }
//    //            }
//    //        }
//    //    }
//
//    override def toString() =
//        partId.toString()
//}
//// 1st dataset objects
//case class Key0(_partId: Int) extends KeyBase(_partId) {}
//// 2nd dataset objects
//case class Key1(_partId: Int) extends KeyBase(_partId) {}
//// 1st dataset objects for Full KNN Join
//// case class Key3(_partId: Int) extends KeyBase(_partId) {}
//
//object TBD {
//    def main(args: Array[String]): Unit = {
//        val lst = /*Sorted*/ Set(Key1(0).asInstanceOf[KeyBase],
//                                 Key1(5).asInstanceOf[KeyBase],
//                                 Key1(-1).asInstanceOf[KeyBase],
//                                 Key0(3).asInstanceOf[KeyBase],
//                                 Key0(2).asInstanceOf[KeyBase],
//                                 Key0(-4).asInstanceOf[KeyBase])
//
//        lst.foreach(println)
//    }
//}
