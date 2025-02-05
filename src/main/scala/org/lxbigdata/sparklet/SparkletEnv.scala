package org.lxbigdata.sparklet

import org.lxbigdata.sparklet.serializer.{JavaSerializer, Serializer}
import org.lxbigdata.sparklet.shuffle.{HashShuffleManager, ShuffleManager}
import org.lxbigdata.sparklet.storage.BlockManager

/**
 * ClassName: SparkletEnv
 * Package: org.lxbigdata.sparklet
 * Description: Sparklet环境类
 *
 * @author lx
 * @version 1.0
 */
class SparkletEnv
(
  val serializer: Serializer,
  val shuffleManager: ShuffleManager,
  val blockManager: BlockManager
){

}

object SparkletEnv {
  private var sparkletEnv: SparkletEnv = _

  def createDriverEnv(sparkletConf: SparkletConf): SparkletEnv = {
    val shuffleManager = new HashShuffleManager()
    val serializer = new JavaSerializer()
    val blockManager = new BlockManager(shuffleManager, sparkletConf)
    val env = new SparkletEnv(serializer, shuffleManager, blockManager)
    env
  }

  def get: SparkletEnv = {
    if (sparkletEnv == null) {
      throw new IllegalStateException("SparkletEnv has not been initialized")
    }
    sparkletEnv
  }

}
