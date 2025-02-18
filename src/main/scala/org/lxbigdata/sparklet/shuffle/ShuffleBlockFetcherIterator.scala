package org.lxbigdata.sparklet.shuffle

import org.apache.commons.io.FileUtils
import org.lxbigdata.sparklet.TaskContext
import org.lxbigdata.sparklet.network.ManagedBuffer
import org.lxbigdata.sparklet.serializer.Serializer
import org.lxbigdata.sparklet.shuffle.ShuffleBlockFetcherIterator.{FetchResult, SuccessFetchResult}
import org.lxbigdata.sparklet.storage.{BlockId, BlockManager, ShuffleBlockId}

import java.io.File
import java.nio.file.{DirectoryStream, Files, Path}
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

/**
 * ClassName: ShuffleBlockFetcherIterator
 * Package: org.lxbigdata.sparklet.shuffle
 * Description:
 *
 * @author lx
 * @version 1.0
 */
class ShuffleBlockFetcherIterator(
                                   context: TaskContext,
                                   blockManager: BlockManager,
                                   serializer: Serializer
                                 ) extends Iterator[(BlockId, Iterator[Any])] {

  var numBlocksToFetch = 0
  var numBlocksProcessed = 0
  @volatile var currentResult: FetchResult = null

  val results = new LinkedBlockingQueue[FetchResult]

  private val localBlocks = new ArrayBuffer[BlockId]()

  initialize()

  def fetchLocalBlocks(): Unit = {
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val blockId = iter.next()
      try {
        val buf = blockManager.getBlockData(blockId)
        buf.retain()
        results.put(new SuccessFetchResult(blockId, 0, buf))
      }
    }
  }

  //
  def splitLocalRemoteBlocks(): Unit = {
    import scala.collection.JavaConverters._
    val files = Files.newDirectoryStream(Path.of(blockManager.getConf.get("sparklet.tmp.dir").get))
    files.asScala.foreach(file => {
      var name = file.toFile.getName
      //shuffle_0_0_0.data
      if (name.endsWith(".data")) {
        name = name.substring(0, name.lastIndexOf("."))
        if (name.endsWith(context.partitionId.toString)) {
          val fields = name.split("_")
          val shuffleId = fields(1).toInt
          val mapId = fields(2).toInt
          val reduceId = fields(3).toInt
          localBlocks += ShuffleBlockId(shuffleId, mapId, reduceId)
        }
      }
    })
  }

  def initialize(): Unit = {
    splitLocalRemoteBlocks()
    fetchLocalBlocks()
  }

  override def hasNext: Boolean = numBlocksProcessed < localBlocks.size

  override def next(): (BlockId, Iterator[Any]) = {
    numBlocksProcessed += 1
    currentResult = results.take()
    val result = currentResult
    val it = result match {
      case SuccessFetchResult(blockId, size, buf) => {
        val is = buf.createInputStream()
        serializer.newInstance().deserializeStream(is).asIterator
      }
    }
    (result.blockId, it)
  }
}

object ShuffleBlockFetcherIterator {
  // FetchResult is used to return the result of fetching a block.
  sealed trait FetchResult {
    val blockId: BlockId
  }

  case class SuccessFetchResult(blockId: BlockId, size: Long, buf: ManagedBuffer) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

}