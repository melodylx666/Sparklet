package org.lxbigdata.sparklet.shuffle
import org.lxbigdata.sparklet.{ShuffleDependency, TaskContext}

/**
 * ClassName: HashShuffleManager
 * Package: org.lxbigdata.sparklet.shuffle
 * Description: hash shuffle版本的实现
 *
 * @author lx
 * @version 1.0   
 */
class HashShuffleManager extends ShuffleManager{

  override def registerShuffle[K, V, C](shuffleId: Int, length: Int, value: ShuffleDependency[K, V, C]): ShuffleHandle = ???

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = ???

  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int, context: TaskContext): ShuffleReader[K, C] = ???

  override def shuffleBlockManager: ShuffleBlockManager = ???

  override def stop(): Unit = ???
}
