package saprklet.utils

import org.junit.jupiter.api.Test
import org.lxbigdata.sparklet.util.{ByteBufferInputStream, ByteBufferOutputStream}
import sun.nio.ch.DirectBuffer

import java.io.ByteArrayInputStream
import java.lang.ref.Cleaner
import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

/**
 * ClassName: ByteBufferIStreamTest01
 * Package: saprklet.utils
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class ByteBufferIStreamTest01 {
  private def clean(buffer: ByteBuffer): Unit = {
    if (buffer.isDirect) {
      val cleaner = buffer.asInstanceOf[DirectBuffer].cleaner()
      cleaner.clean()
    }
  }
  @Test
  def test01(): Unit = {
    val offHeapBuffer = ByteBuffer.allocateDirect(1024)

    val str = "hello world".getBytes(StandardCharsets.UTF_8)
    offHeapBuffer.put(str)

    offHeapBuffer.flip()

    val i = offHeapBuffer.remaining()
    val arr = new Array[Byte](i)
    offHeapBuffer.get(arr)

    val result = new String(arr, StandardCharsets.UTF_8)
    assert(result == "hello world")
  }
  @Test
  def test02(): Unit = {
    val offHeapBuffer = ByteBuffer.allocateDirect(1024)
    val str = "hello world".getBytes(StandardCharsets.UTF_8)
    offHeapBuffer.put(str)

    offHeapBuffer.flip() //一定要调用flip()切换一下

    val stream = new ByteBufferInputStream(offHeapBuffer)
    val bytes = stream.readAllBytes()
    val result = new String(bytes, StandardCharsets.UTF_8)

    assert(result == "hello world")
  }
  @Test
  def test03(): Unit = {
    val stream = new ByteBufferOutputStream()
    val str = "hello world".getBytes(StandardCharsets.UTF_8)
    stream.write(str)
    stream.close()
    val buffer = stream.toByteBuffer //直接复用原有数组，获取ByteBuffer
    val bytes = buffer.array().takeWhile(_ != 0)
    val result = new String(bytes, StandardCharsets.UTF_8)

    assert(result == "hello world")
  }
}
