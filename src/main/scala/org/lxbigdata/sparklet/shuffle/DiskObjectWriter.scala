package org.lxbigdata.sparklet.shuffle

import org.lxbigdata.sparklet.serializer.{SerializationStream, Serializer}
import org.lxbigdata.sparklet.storage.BlockId

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream}
import java.nio.channels.FileChannel


/**
 * ClassName: DiskObjectWriter
 * Package: org.lxbigdata.sparklet.shuffle
 * Description: 刷盘
 *
 * @author lx
 * @version 1.0   
 */
class DiskObjectWriter
(
 blockId:BlockId,
 file:File,
 serializer:Serializer,
 bufferSize:Int
){
  private var fos:FileOutputStream = _
  private var bs:OutputStream = _
  private var objOut:SerializationStream = _
  private var channel:FileChannel = _
  private var initialized = false
  private var numRecordsWritten = 0
  //初始化
  def open():DiskObjectWriter = {
    fos = new FileOutputStream(file,true)
    bs = new BufferedOutputStream(fos,bufferSize)
    channel = fos.getChannel
    //使用可切换的序列化方式进行对象序列化
    objOut = serializer.newInstance().serializeStream(bs)
    initialized = true
    this
  }
  //写入数据
  def write(value:Any):Unit = {
    if(!initialized){
      open()
    }
    objOut.writeObject(value)
    numRecordsWritten += 1
  }
  //关闭资源
  //todo 使用scala2.13的using管理资源
  def commitAndClose(): Unit = {
    if (initialized) {
      objOut.flush()
      bs.flush()
      objOut.close()
      channel = null
      bs = null
      fos = null
      objOut = null
      initialized = false
    }
  }
}
