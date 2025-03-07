package org.lxbigdata.sparklet.util

/**
 * ClassName: ByteBufferInputStream
 * Package: org.lxbigdata.sparklet.util
 * Description:
 * 和FileInputStream类似，但是从ByteBuffer中读取数据。
 * read方法返回的是成功的读取数量，-1表示已经读取完
 *
 *
 * @author lx
 * @version 1.0   
 */
import java.io.InputStream
import java.nio.ByteBuffer

class ByteBufferInputStream(private var buffer: ByteBuffer)
  extends InputStream {

  override def read(): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      buffer.get() & 0xFF
    }
  }

  override def read(dest: Array[Byte]): Int = {
    read(dest, 0, dest.length)
  }

  override def read(dest: Array[Byte], offset: Int, length: Int): Int = {
    if (buffer == null || buffer.remaining() == 0) {
      cleanUp()
      -1
    } else {
      val amountToGet = math.min(buffer.remaining(), length) //(limit - pos) min (len)
      buffer.get(dest, offset, amountToGet)
      amountToGet
    }
  }

  override def skip(bytes: Long): Long = {
    if (buffer != null) {
      val amountToSkip = math.min(bytes, buffer.remaining).toInt
      buffer.position(buffer.position() + amountToSkip)
      if (buffer.remaining() == 0) {
        cleanUp()
      }
      amountToSkip
    } else {
      0L
    }
  }

  /**
   * Clean up the buffer, and potentially dispose of it using StorageUtils.dispose().
   */
  private def cleanUp(): Unit = {
    if (buffer != null) {
      buffer = null
    }
  }
}
