package org.lxbigdata.sparklet.shuffle

import org.lxbigdata.sparklet.TaskContext
import org.lxbigdata.sparklet.scheduler.MapStatus

/**
 * ClassName: ShuffleWriter
 * Package: org.lxbigdata.sparklet.shuffle
 * Description: Shuffle写接口
 *
 * @author lx
 * @version 1.0   
 */
trait ShuffleWriter[K,V] {
  //do shuffle write
  def write(records: Iterator[_ <: Product2[K, V]]): Unit

  //stop shuffle writer and return status
  def stop(success:Boolean):Option[MapStatus]
}

class HashShuffleWriter[K,V]
(
  shuffleBlockManager: ShuffleBlockManager,
  handle: BaseShuffleHandle[K,V,_],
  mapId: Int,
  context: TaskContext
) extends ShuffleWriter[K,V] {

  private val dep = handle.dependency
  private val numOutputSplits = dep.partitioner.numPartitions
  private val ser = dep.serializer

  //获取对应的shuffleGroup,这里一个task一个Group
  private val shuffle = shuffleBlockManager.forMapTask(dep.shuffleId, mapId, numOutputSplits, ser)

  override def write(records: Iterator[_ <: Product2[K, V]]): Unit = {
    //真正使用分区器的地方
    //每个record都写入分区粒度的桶中
    for(record <- records){
      val bucketId = dep.partitioner.getPartition(record._1)
      shuffle.writers(bucketId).write(record)
    }
  }

  //停止并返回MapStatus
  override def stop(success: Boolean): Option[MapStatus] = {
    shuffle.writers.foreach(w => w.commitAndClose())
    Some(MapStatus(true))
  }
}
