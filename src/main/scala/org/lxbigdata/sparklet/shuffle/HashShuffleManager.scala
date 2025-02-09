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
  private def fileShuffleBlockManager = new FileShuffleBlockManager()
  override def registerShuffle[K, V, C](shuffleId: Int, length: Int, value: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new BaseShuffleHandle[K, V, C](shuffleId, length, value)
  }

  override def getWriter[K, V](handle: ShuffleHandle, mapId: Int, context: TaskContext): ShuffleWriter[K, V] = {
    new HashShuffleWriter[K, V](fileShuffleBlockManager,handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context)
  }

  override def getReader[K, C](handle: ShuffleHandle, startPartition: Int, endPartition: Int, context: TaskContext): ShuffleReader[K, C] = {
    ???
  }

  override def shuffleBlockManager: ShuffleBlockManager = ???

  override def stop(): Unit = ???
}
