package org.lxbigdata.sparklet.shuffle

import org.lxbigdata.sparklet.{ShuffleDependency, TaskContext}

/**
 * ClassName: ShuffleManager
 * Package: org.lxbigdata.sparklet.shuffle
 * Description: shuffle manager接口,管理shuffle类资源
 *
 * @author lx
 * @version 1.0   
 */
trait ShuffleManager {
  //注册shuffle
  def registerShuffle[K,V,C]
  (
    shuffleId: Int,
    length: Int,
    value: ShuffleDependency[K, V, C]
  ):ShuffleHandle

  //获取对应的shuffleWriter资源
  def getWriter[K,V]
  (
    handle: ShuffleHandle,
    mapId: Int,
    context: TaskContext
  ):ShuffleWriter[K,V]

  def getReader[K,C]
  (
    handle: ShuffleHandle,
    startPartition: Int, //开始分区
    endPartition: Int, //结束分区
    context: TaskContext
  ):ShuffleReader[K,C]

  //shuffle block管理器，能够根据block获取block的数据迭代器，从而将其拉取到reduce端，详见unit-test
  def shuffleBlockManager:ShuffleBlockManager

  def stop():Unit

}
