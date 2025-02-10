package org.lxbigdata.sparklet.shuffle

import org.lxbigdata.sparklet.TaskContext
import org.lxbigdata.sparklet.serializer.Serializer
import org.lxbigdata.sparklet.util.InterruptibleIterator

/**
 * ClassName: shuffleReader
 * Package: org.lxbigdata.sparklet.shuffle
 * Description: shuffle reader接口
 * 在设计文档里面已经说明了将流toIter的重要性，其实就在这里
 *
 * @author lx
 * @version 1.0   
 */
trait ShuffleReader[K,C] {
  //以迭代器方式拉取上游磁盘中的数据
  def read():Iterator[Product2[K,C]]
}
class HashShuffleReader[K,C](
  handle:BaseShuffleHandle[K,_,C],
  startPartition:Int,
  endPartition:Int,
  context:TaskContext
) extends ShuffleReader[K,C] {

  private val dep = handle.dependency

  override def read(): Iterator[Product2[K, C]] = {
    //获取序列化器
    val ser = Serializer.getSerializer(dep.serializer)
    //拉取对应block的数据的迭代器
    val iter = BlockStoreShuffleFetcher.fetch(handle.shuffleId, startPartition, context, ser)
    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined && dep.mapSideCombine) {
      new InterruptibleIterator(context, dep.aggregator.get.combineValuesByKey(iter, context)).asInstanceOf[Iterator[Product2[K, C]]]
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      iter.asInstanceOf[Iterator[Product2[K, C]]].map(pair => (pair._1, pair._2))
    }
    aggregatedIter
  }

}
