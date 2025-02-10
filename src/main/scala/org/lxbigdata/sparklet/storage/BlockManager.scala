package org.lxbigdata.sparklet.storage

import org.lxbigdata.sparklet.SparkletConf
import org.lxbigdata.sparklet.network.ManagedBuffer
import org.lxbigdata.sparklet.serializer.Serializer
import org.lxbigdata.sparklet.shuffle.{DiskObjectWriter, ShuffleManager}

import java.io.File
import java.nio.file.Path

/**
 * ClassName: BlockManager
 * Package: org.lxbigdata.sparklet.storage
 * Description: 
 * Description: Manager running on every node (driver and executors)
 * which provides interfaces for putting and retrieving blocks both locally and remotely
 * into various stores (memory, disk, and off-heap).
 *
 * @author lx
 * @version 1.0   
 */
class BlockManager(shuffleManager: ShuffleManager,conf:SparkletConf) {
  private var curConf:SparkletConf = _
  def getConf = {
    if(curConf == null){
      curConf = conf
    }
    curConf
  }

  def getWriter
  (
    blockId: BlockId,
    file: File,
    serializer: Serializer,
    bufferSize: Int
  ): DiskObjectWriter = {
    new DiskObjectWriter(blockId, file, serializer, bufferSize)
  }

  def getDataFile(blockId: BlockId):File = {
    //todo 这里文件名的问题
    new File(conf.get("sparklet.tmp.dir").get + blockId.name + ".data")
  }
  def getIndexFile(blockId: BlockId):File = {
    new File(conf.get("sparklet.tmp.dir").get + blockId.name + ".index")
  }

  //对应block的输入流
  def getBlockData(blockId: BlockId): ManagedBuffer = {
    shuffleManager.shuffleBlockManager.getBlockData(blockId.asInstanceOf[ShuffleBlockId])
  }
}
