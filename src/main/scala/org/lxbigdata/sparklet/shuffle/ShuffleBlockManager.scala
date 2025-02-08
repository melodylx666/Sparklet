package org.lxbigdata.sparklet.shuffle

import org.lxbigdata.sparklet.network.ManagedBuffer
import org.lxbigdata.sparklet.serializer.Serializer
import org.lxbigdata.sparklet.storage.ShuffleBlockId

/**
 * ClassName: ShuffleBlockManager
 * Package: org.lxbigdata.sparklet.shuffle
 * Description: 管理shuffle数据块读写
 *
 * @author lx
 * @version 1.0   
 */
trait ShuffleBlockManager {

  def forMapTask(shuffleId:Int,mapId:Int,numBuckets:Int,serializer:Serializer):ShuffleWriterGroup
  // 获取shuffle数据块对应的输入流
  def getBlockData(blockId:ShuffleBlockId):ManagedBuffer
  def stop():Unit
}
