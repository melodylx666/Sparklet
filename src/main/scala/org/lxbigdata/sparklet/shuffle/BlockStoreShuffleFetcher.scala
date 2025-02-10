package org.lxbigdata.sparklet.shuffle

import org.lxbigdata.sparklet.network.ManagedBuffer
import org.lxbigdata.sparklet.{SparkletEnv, TaskContext}
import org.lxbigdata.sparklet.serializer.Serializer
import org.lxbigdata.sparklet.storage.{BlockId, BlockManager}

/**
 * ClassName: BlockStoreShuffleFetcher
 * Package: org.lxbigdata.sparklet.shuffle
 * Description:
 *
 * @author lx
 * @version 1.0   
 */
object BlockStoreShuffleFetcher {
  def fetch[T](shuffleId:Int,reduceId:Int,context:TaskContext,serializer:Serializer):Iterator[T] = {
    val manager: BlockManager = SparkletEnv.get.blockManager
    val blockFetcherItr: ShuffleBlockFetcherIterator = new ShuffleBlockFetcherIterator(context, manager, serializer)
    //todo 这里阻塞
    blockFetcherItr.flatMap(x => x._2).asInstanceOf[Iterator[T]]
  }
}


