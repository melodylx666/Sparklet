package org.lxbigdata.sparklet.util

/**
 * ClassName: ByteBufferOutputStream
 * Package: lx.Spark.util
 * Description:  Provide a zero-copy way to convert data in ByteArrayOutputStream to ByteBuffer
 *
 * @author lx
 * @version 1.0
 */

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

class ByteBufferOutputStream(capacity: Int) extends ByteArrayOutputStream(capacity) {

  def this() = this(32) // default capacity is 32

  def getCount(): Int = count

  private[this] var closed: Boolean = false

  override def write(b: Int): Unit = {
    require(!closed, "cannot write to a closed ByteBufferOutputStream")
    super.write(b)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    require(!closed, "cannot write to a closed ByteBufferOutputStream")
    super.write(b, off, len)
  }

  override def reset(): Unit = {
    require(!closed, "cannot reset a closed ByteBufferOutputStream")
    super.reset()
  }

  override def close(): Unit = {
    if (!closed) {
      super.close()
      closed = true
    }
  }

  def toByteBuffer: ByteBuffer = {
    require(closed, "can only call toByteBuffer() after ByteBufferOutputStream has been closed")
    ByteBuffer.wrap(buf, 0, count)
  }
}
