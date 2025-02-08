package org.lxbigdata.sparklet.shuffle

/**
 * ClassName: shuffleReader
 * Package: org.lxbigdata.sparklet.shuffle
 * Description: shuffle reader接口
 * 在设计文档里面已经说明了将流toIter的重要性，其实就在这里
 * @author lx
 * @version 1.0   
 */
trait ShuffleReader[K,C] {
  //以迭代器方式拉取上游磁盘中的数据
  def read():Iterator[Product2[K,C]]
}
