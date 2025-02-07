package saprklet.rddClass

import org.junit.jupiter.api.Test

/**
 * ClassName: otherTest01
 * Package: saprklet.rddClass
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class otherTest01 {
  @Test
  def test01(): Unit ={
    implicit def charToInt(c: Char) = c.toInt
    val a = 'a'

    val b = implicitly[Int](a)
    println(b)
  }

}
