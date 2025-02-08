package org.lxbigdata.sparklet.serializer

import org.lxbigdata.sparklet.util.{ByteBufferInputStream, ByteBufferOutputStream}

import java.io.{InputStream, ObjectInputStream, ObjectOutputStream, ObjectStreamClass, OutputStream}
import java.nio.ByteBuffer
import scala.reflect.ClassTag

/**
 * ClassName: JavaSerializer
 * Package: org.lxbigdata.sparklet.serializer
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class JavaSerializer extends Serializer {
  private val counterReset = 100
  private val extraDebugInfo = true

  override def newInstance(): SerializerInstance = {
    val classLoader = Thread.currentThread.getContextClassLoader
    return new JavaSerializerInstance(counterReset, extraDebugInfo, classLoader)
  }
}
class JavaSerializerInstance
(
  counterReset: Int,
  extraDebugInfo: Boolean,
  defaultClassLoader: ClassLoader
) extends SerializerInstance {
  //序列化，数据先进入到ByteBufferOutputStream中，再通过ByteBufferOutputStream.toByteBuffer转换为ByteBuffer
  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    val bos = new ByteBufferOutputStream()
    val out = serializeStream(bos)
    out.writeObject(t)
    out.close()
    bos.toByteBuffer
  }
  //反序列化，从ByteBuffer中读取数据流
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = {
    val bis = new ByteBufferInputStream(bytes)
    val in = deserializeStream(bis)
    in.readObject()
  }

  override def serializeStream(s: OutputStream): SerializationStream = {
    new JavaSerializationStream(s, counterReset, extraDebugInfo)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new JavaDeserializationStream(s, defaultClassLoader)
  }

}

//装饰器模式
class JavaSerializationStream(out: OutputStream, counterReset: Int, extraDebugInfo: Boolean)
  extends SerializationStream {

  private val objOut = new ObjectOutputStream(out)
  private var counter = 0

  def writeObject[T: ClassTag](t: T): SerializationStream = {
    try {
      objOut.writeObject(t)
    } catch {
      case e => throw new Exception("Exception while serializing object " + t, e)
    }
    counter += 1
    if (counterReset > 0 && counter >= counterReset) {
      objOut.reset()
      counter = 0
    }
    this
  }

  def flush(): Unit = {
    objOut.flush()
  }

  def close(): Unit = {
    objOut.close()
  }
}
//同样的装饰器模式设计
class JavaDeserializationStream(in: InputStream, loader: ClassLoader) extends DeserializationStream {

  private val objIn = new ObjectInputStream(in) {

    override def resolveClass(desc: ObjectStreamClass): Class[_] =
      try {
        // scalastyle:off classforname
        Class.forName(desc.getName, false, loader)
        // scalastyle:on classforname
      } catch {
        case e: ClassNotFoundException =>
          JavaDeserializationStream.primitiveMappings.getOrElse(desc.getName, throw e)
      }

    override def resolveProxyClass(ifaces: Array[String]): Class[_] = {
      // scalastyle:off classforname
      val resolved = ifaces.map(iface => Class.forName(iface, false, loader))
      // scalastyle:on classforname
      java.lang.reflect.Proxy.getProxyClass(loader, resolved: _*)
    }
  }

  def readObject[T: ClassTag](): T = objIn.readObject().asInstanceOf[T]

  def close(): Unit = {
    objIn.close()
  }
}

private object JavaDeserializationStream {

  val primitiveMappings = Map[String, Class[_]](
    "boolean" -> classOf[Boolean],
    "byte" -> classOf[Byte],
    "char" -> classOf[Char],
    "short" -> classOf[Short],
    "int" -> classOf[Int],
    "long" -> classOf[Long],
    "float" -> classOf[Float],
    "double" -> classOf[Double],
    "void" -> classOf[Unit])

}


