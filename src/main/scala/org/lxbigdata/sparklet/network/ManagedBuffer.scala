package org.lxbigdata.sparklet.network

import com.google.common.io.ByteStreams

import java.io.{File, FileInputStream, InputStream}

/**
 * ClassName: ManagedBuffer
 * Package: org.lxbigdata.sparklet.network
 * Description: This interface provides an immutable view for data in the form of bytes
 * 底层的数据可能是可以文件buffer，或者NIOBuffer
 * @author lx
 * @version 1.0   
 */
abstract class ManagedBuffer {

  def createInputStream(): InputStream

  def retain(): ManagedBuffer

}

// 文件段读取流
class FileSegmentManagedBuffer(val file: File, val offset: Long, val length: Long) extends ManagedBuffer {
  //从文件流读取任意一段
  override def createInputStream(): InputStream = {
    val is = new FileInputStream(file)
    ByteStreams.skipFully(is,offset)
    new LimitedInputStream(is,length)
  }

  override def retain(): ManagedBuffer = {
    this
  }

}
