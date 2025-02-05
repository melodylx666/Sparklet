package saprklet.baseClass

import org.junit.jupiter.api.Test
import org.lxbigdata.sparklet.{SparkletConf, SparkletContext}

/**
 * ClassName: sparkletContextTest01
 * Package: saprklet.baseClass
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class sparkletContextTest01 {
  @Test
  def test01(): Unit = {
    val sparklet = new SparkletConf()
      .setMaster("local")
      .setAppName("SparkletConfTest01")
    val sparkletContext = new SparkletContext(sparklet)

    assert(sparkletContext.getEnv != null)
  }

}
