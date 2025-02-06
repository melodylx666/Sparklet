# Sparklet核心设计

## 工具类设计

### Stream of ByteBuffer

#### off-heap的好处

以Linux平台为例子，JVM是一个进程，`CPU`切换到用户态的时候，只能访问到虚拟地址空间内的限定区域。如果需要执行内核代码，就必须将`CPU`切换为内核态，然后`trap`让其跳转到虚拟地址空间的`内核`代码与数据区域。

上述的CPU从内核态到用户态的转换，开销是寄存器状态，以及栈的状态的存储，其实并没有很多人所说的上下文切换。

目前看起来开销可以接受。

但是当需要网络获取数据的时候，`CPU`处于用户等级的时候并无法操作这个数据，必须先切换到内核态，然后`CPU`将数据从全局内存，复制到该进程的地址空间的管理的内存区域，这个开销是很大的。

所以`off-heap`的基本实现，就是首先在系统内存分配区域，然后在用户进程中分配一个代表`off-heap`区域的虚拟地址，这样当用户态的`CPU`访问该虚拟地址的时候，经过`MMU`映射，可以映射到之前分配过的系统内存区域。

#### 和ByteArrayxxxStream

Java IO 已经有了`ByteArray`流，为什么还要使用`ByteBuffer`流呢？

这其实涉及到框架的管理问题。如果一个流的来源只是`on-heap`，那么使用`byte[]`足够了。

但是大数据计算框架等通常出于GC，或者开销问题，会使用到堆外内存，也就是`off-heap`。

这个时候有一个统一管理的API就很重要了，并且，可以使用流的方式管理块粒度的IO，所以就有了`Stream of ByteBuffer`。

#### 具体设计

> [java - Wrapping a ByteBuffer with an InputStream - Stack Overflow](https://stackoverflow.com/questions/4332264/wrapping-a-bytebuffer-with-an-inputstream)

对于`ByteBufferInputStream`，需要注意，从该流读取数据之前必须`filp`，因为没有内置的`filp`逻辑。

对于`ByteBufferOutputStream`，就是一个`ByteArrayOutputStream`，但是它的`toByteBuffer`直接复用了原有的数组，减去了读出再写的开销，可以视作零拷贝。

```java
    val stream = new ByteBufferOutputStream()
    val str = "hello world".getBytes(StandardCharsets.UTF_8)
    stream.write(str)
    stream.close()
    val buffer = stream.toByteBuffer //直接复用原有数组，获取ByteBuffer
    val bytes = buffer.array().takeWhile(_ != 0)
    val result = new String(bytes, StandardCharsets.UTF_8)

    assert(result == "hello world")
```

```java
    val offHeapBuffer = ByteBuffer.allocateDirect(1024)
    val str = "hello world".getBytes(StandardCharsets.UTF_8)
    offHeapBuffer.put(str)

    offHeapBuffer.flip() //一定要调用flip()切换一下

    val stream = new ByteBufferInputStream(offHeapBuffer)
    val bytes = stream.readAllBytes()
    val result = new String(bytes, StandardCharsets.UTF_8)

    assert(result == "hello world")
```

#### 经典问题与off-heap管理

> [Java堆外内存之三：堆外内存回收方法 - duanxz - 博客园 (cnblogs.com)](https://www.cnblogs.com/duanxz/p/6089485.html)

on-heap分配的内存受GC管理，那off-heap的程序运行时的管理受GC影响较小，必然要涉及到`unsafe-api`的操作。

很多人在使用大数据计算框架,更新到`JDK17`的时候,会遇到关于`unsafe`的API管理报错根本原因就是这里的管理使用了相关API。

具体管理函数设计如下:

```java
  private def clean(buffer: ByteBuffer): Unit = {
    if (buffer.isDirect) {
      val cleaner = buffer.asInstanceOf[DirectBuffer].cleaner()
      cleaner.clean()
    }
  }

```

当然，一个`clean`函数的注册是需要具体对象，以及操作函数的，`JDK-nio`中的处理函数对象如下:

```java
    private record Deallocator(long address, long size, int capacity) implements Runnable {
        private Deallocator {
            assert address != 0;
        }

        public void run() {
            UNSAFE.freeMemory(address);
            Bits.unreserveMemory(size, capacity);
        }
    }
```

在具体设计的时候，就可以根据是否要真正清理而选择上述的`unsafe-api`，而不是使用并不真正清理数据的`ByteBuffer::clear`。

`Spark`框架在具体的`Storage-util`的工具类中就有类似实现，所以会导致用户升级`JDK`之后出问题。

### Next Iterator

#### 增强之处

[内存有限的情况下 Spark 如何处理 T 级别的数据？ - 知乎 (zhihu.com)](https://www.zhihu.com/question/23079001)

其实流式迭代器正是人们宣扬的`Spark`的懒惰加载，或者说是`RDD定义逻辑不进行计算`的根本，和全部读取到内存相比，其好处是很明显的。**它的真正实现并不是人们认为的`lazy关键字`方式实现**。这点后面会专门出文章。

其暴露出很少的构成迭代器的方法，以及关闭的逻辑给用户需要管理进行实现。

用户可以将任何其想进行管理的资源转换为迭代器方式进行读取。

#### 自定义

```java
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
```

我们可以自己写一个小例子来感受功能：

```java
  def test01(): Unit = {
    val iter = new NextIterator[Int] {
      override protected def getNext(): Int = {
        (math.random()*6).toInt
      }
      override protected def close(): Unit = {
        println("close")
      }
    }
    var ans = 0
    if(iter.hasNext){
      ans = iter.next()
    }
    iter.closeIfNeeded()
    println(ans)
    assert(ans >= 0)
  }
```



## 序列化器设计

### 序列化trait

#### 序列化流

首先是很常规的`序列化流`

```java
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
```

序列化流唯一需要功能就是将对象序列化之后写出去，当然这里提供了一个默认实现来将迭代器的数据全部写出去。

**核心是反序列化器**

为什么，想一想大数据引擎中必须要谈的`Shuffle`。

上面已经说过,`Spark`的实现中，数据是通过`RDD`上`Compute`闭包的堆叠计算之后，计算出最终结果的。那我们如何将含有`Shuffle`的前后数据通过迭代器串联起来呢？

实现方式就是将`fetch/源数据`的数据同样转换为迭代器，这样就可以将一个阶段，甚至多个阶段的内的逻辑串联起来了。

```java
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
  
```

刚好也用上了我上面写的小`demo`。


### 序列化实例

#### 自定义类加载

自定义的类的序列化和反序列化，依旧需要使用`ObjectStream`，但是需要重写方法，并提供类加载器。

当加载失败的时候，需要回退到常规类型搜索一遍，然后抛出异常。

映射方式如下：

```java
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
```
