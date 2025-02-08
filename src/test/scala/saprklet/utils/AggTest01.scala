package saprklet.utils

import org.junit.jupiter.api.Test
import org.lxbigdata.sparklet.{SparkletConf, SparkletContext, SparkletEnv}
import org.lxbigdata.sparklet.util.Aggregator

/**
 * ClassName: AggTest01
 * Package: saprklet.utils
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class AggTest01 {
  @Test
  def test01():Unit = {
    val createCombiner = (v:Int) => v
    val mergeValue = (c:Int,v:Int) => c + v
    val mergeCombiner = (c1:Int,c2:Int) => c1 + c2
    val agg = new Aggregator[Int,Int,Int](createCombiner,mergeValue,mergeCombiner)
    val iter = List((1,1),(1,2),(2,3)).iterator
  }

}
