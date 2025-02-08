package saprklet.networkClass

import org.junit.jupiter.api.Test
import org.lxbigdata.sparklet.network.FileSegmentManagedBuffer
import org.lxbigdata.sparklet.serializer.{DeserializationStream, JavaDeserializationStream, JavaSerializationStream, SerializationStream}

import java.nio.file.{Files, Path}
import scala.reflect.ClassTag

/**
 * ClassName: FSBufferTest01
 * Package: saprklet.networkClass
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class FSBufferTest01 {
  @Test
  def test01(): Unit = {
    //创建文件并写入数据
    val path = Files.createFile(Path.of("test.txt"))
    Files.write(path,"hello world".getBytes)

    val buffer = new FileSegmentManagedBuffer(path.toFile.getPath, 6,5) //offset + length
    val buffer1 = buffer.retain()
    val stream = buffer1.createInputStream()
    val bytes = stream.readAllBytes()
    val str = new String(bytes)

    assert(str == "world")
    stream.close()
    Files.deleteIfExists(path)
  }
  @Test
  def test02(): Unit = {
    //写入序列化数据
    val path = Files.createFile(Path.of("test.txt"))
    val outputStream = Files.newOutputStream(path)
    val str = Array[Int](1,2,3)
    val serializationStream = new JavaSerializationStream(outputStream, 100, true)
    serializationStream.writeObject(str)
    //创建文件段读取流,并包装为反序列化流
    val buffer = new FileSegmentManagedBuffer(path.toFile.getPath, 0,100) //offset + length
    val buffer1 = buffer.retain()
    val stream = buffer1.createInputStream()

    val deserializationStream = new JavaDeserializationStream(stream, Thread.currentThread.getContextClassLoader)
    val iterator = deserializationStream.asIterator

    assert(iterator.next().asInstanceOf[Array[Int]].mkString(",") == "1,2,3")
    stream.close()
    Files.deleteIfExists(path)
  }

}
