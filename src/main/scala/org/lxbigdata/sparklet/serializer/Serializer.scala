package org.lxbigdata.sparklet.serializer

import org.lxbigdata.sparklet.SparkletEnv
import org.lxbigdata.sparklet.util.{ByteBufferInputStream, NextIterator}

import java.io.{ByteArrayOutputStream, EOFException, InputStream, OutputStream}
import java.nio.ByteBuffer
import scala.reflect.ClassTag

/**
 * ClassName: Serializer
 * Package: org.lxbigdata.sparklet.serializer
 * Description: spark序列化器, copy from apache/spark
 *
 * @author lx
 * @version 1.0   
 */
trait Serializer {
  def newInstance(): SerializerInstance
}


object Serializer {
  def getSerializer(serializer: Serializer): Serializer = {
    if (serializer == null) SparkletEnv.get.serializer else serializer
  }
}

trait SerializerInstance {
  def serialize[T: ClassTag](t: T): ByteBuffer

  def deserialize[T: ClassTag](bytes: ByteBuffer): T

  def serializeStream(s: OutputStream): SerializationStream

  def deserializeStream(s: InputStream): DeserializationStream

  def serializeMany[T: ClassTag](iterator: Iterator[T]): ByteBuffer = {
    // Default implementation uses serializeStream
    val stream = new ByteArrayOutputStream()
    serializeStream(stream).writeAll(iterator)
    val buffer = ByteBuffer.wrap(stream.toByteArray)
    buffer.flip()
    buffer
  }

  def deserializeMany(buffer: ByteBuffer): Iterator[Any] = {
    // Default implementation uses deserializeStream
    buffer.rewind()
    deserializeStream(new ByteBufferInputStream(buffer)).asIterator
  }
}

/**
 * :: DeveloperApi ::
 * A stream for writing serialized objects.
 */
trait SerializationStream {
  def writeObject[T: ClassTag](t: T): SerializationStream
  def flush(): Unit
  def close(): Unit

  def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
    while (iter.hasNext) {
      writeObject(iter.next())
    }
    this
  }
}


/**
 * :: DeveloperApi ::
 * A stream for reading serialized objects.
 */
trait DeserializationStream {
  def readObject[T: ClassTag](): T
  def close(): Unit

  /**
   * Read the elements of this stream through an iterator. This can only be called once, as
   * reading each element will consume data from the input source.
   */
  def asIterator: Iterator[Any] = new NextIterator[Any] {
    override protected def getNext() = {
      try {
        readObject[Any]()
      } catch {
        case eof: EOFException =>
          finished = true
      }
    }

    override protected def close() {
      DeserializationStream.this.close()
    }
  }
}

