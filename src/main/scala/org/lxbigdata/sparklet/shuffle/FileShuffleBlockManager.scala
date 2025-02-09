package org.lxbigdata.sparklet.shuffle
import org.lxbigdata.sparklet.SparkletEnv
import org.lxbigdata.sparklet.network.{FileSegmentManagedBuffer, ManagedBuffer}
import org.lxbigdata.sparklet.serializer.Serializer
import org.lxbigdata.sparklet.storage.ShuffleBlockId

import java.nio.file.{Files, Path}

/**
 * ClassName: FileShuffleBlockManager
 * Package: org.lxbigdata.sparklet.shuffle
 * Description: 创建并维护index和data文件之间的映射
 *
 * @author lx
 * @version 1.0   
 */
class FileShuffleBlockManager extends ShuffleBlockManager {
  val bufferSize = 32* 1024

  private lazy val blockManager = SparkletEnv.get.blockManager

  override def forMapTask(shuffleId: Int, mapId: Int, numBuckets: Int, serializer: Serializer): ShuffleWriterGroup = {
    new ShuffleWriterGroup {
      override val writers: Array[DiskObjectWriter] = {
        Array.tabulate[DiskObjectWriter](numBuckets){
          case bucketId => {
            val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
            //数据文件
            val dataFile = blockManager.getDataFile(blockId)
            Files.deleteIfExists(dataFile.toPath)
            Files.createFile(dataFile.toPath)
            //索引文件
            val indexFile = blockManager.getIndexFile(blockId)
            Files.deleteIfExists(indexFile.toPath)
            Files.createFile(indexFile.toPath)

            blockManager.getWriter(blockId,dataFile,serializer,bufferSize)
          }
        }
      }

      override def releaseWriters(success: Boolean): Unit = ???
    }
  }
  //返回对应blockId的IO流
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    val file = blockManager.getDataFile(blockId)
    new FileSegmentManagedBuffer(file.getName, 0, file.length)
  }

  override def stop(): Unit = ???
}
