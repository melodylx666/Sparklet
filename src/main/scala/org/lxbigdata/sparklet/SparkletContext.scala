package org.lxbigdata.sparklet

import java.util.concurrent.atomic.AtomicInteger

/**
 * ClassName: SparkletContext
 * Package: org.lxbigdata.sparklet
 * Description: Sparklet任务上下文信息
 *
 * @author lx
 * @version 1.0
 */
class SparkletContext(sparkletConf: SparkletConf) {

  private val nextRDDId = new AtomicInteger(0)
  private val nextShuffleId = new AtomicInteger(0)
  private val stopped = new AtomicInteger(0)

  private val env: SparkletEnv = SparkletEnv.createDriverEnv(sparkletConf)


  def newRDDId(): Int = {
    nextRDDId.getAndIncrement()
  }
  def newShuffleId(): Int = {
    nextShuffleId.getAndIncrement()
  }
  def getEnv: SparkletEnv = {
    env
  }
}
