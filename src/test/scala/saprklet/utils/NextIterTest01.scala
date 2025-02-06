package saprklet.utils

import org.junit.jupiter.api.Test
import org.lxbigdata.sparklet.util.NextIterator

/**
 * ClassName: NextIterTest01
 * Package: saprklet.utils
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class NextIterTest01 {
  @Test
  def test01(): Unit = {
    val iter = new NextIterator[Int] {
      override protected def getNext(): Int = {
        (math.random()*6).toInt
      }
      override protected def close(): Unit = {
        println("close")
      }
    }
    var ans = 0
    if(iter.hasNext){
      ans = iter.next()
    }
    iter.closeIfNeeded()
    println(ans)
    assert(ans >= 0)
  }

}
