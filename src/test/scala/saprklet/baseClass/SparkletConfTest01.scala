package saprklet.baseClass

import org.junit.jupiter.api.Test
import org.lxbigdata.sparklet.SparkletConf

/**
 * ClassName: SparkletConfTest01
 * Package: saprklet.baseClass
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class SparkletConfTest01 {
  @Test
  def test(): Unit = {
    val spark = new SparkletConf()
      .setMaster("local")
      .setAppName("SparkletConfTest01")

    assert(spark.get("sparklet.master").get == "local")
  }
}
