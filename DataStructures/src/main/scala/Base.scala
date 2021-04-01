
class Base[T: Numeric] {

  var x: T = _
  var y: T = _

  def this(x: T, y: T) = {

    this()

    this.x = x
    this.y = y
  }

  def this(other: Base[T]) =
    this(other.x, other.y)
}

class Child[T: Numeric] extends Base[T] {

  def this(x: T, y: T) = {
    this()
    this.x = x
    this.y = y
  }

  def this(other: Child[T]) =
    this(other.x, other.y)
}

object Test {
  def main(args: Array[String]): Unit = {

    val c1 = new Child(5, 5)
    val c2 = new Child(5.0, 5.0)
//    val c3 = new Child(AnyRef, AnyRef)

  }
}