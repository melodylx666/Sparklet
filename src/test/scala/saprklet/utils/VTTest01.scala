package saprklet.utils

import org.junit.jupiter.api.Test

import java.util.concurrent.{Callable, Executors}

/**
 * ClassName: VTTest01
 * Package: saprklet.utils
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class VTTest01 {
  @Test
  def test01(): Unit = {
    val factory = Thread.ofVirtual().factory()
//    val service = Executors.newThreadPerTaskExecutor(factory)
    val service = Executors.newCachedThreadPool()
    val startTime = System.currentTimeMillis()
    (1 to 50000).foreach(i => {
      service.submit(new Callable[Int] {
        override def call():Int = {
          Thread.sleep(1000)
          1
        }
      })
    })
    service.close()
    val endTime = System.currentTimeMillis()
    val diff = (endTime - startTime) / 1000
    println(s"耗时：${diff}s")
  }

  @Test
  def test02(): Unit = {
    val service = Executors.newVirtualThreadPerTaskExecutor()
    val future = service.submit(new Runnable {
      override def run(): Unit = {
        Thread.sleep(5000)
        println("hello")
      }
    })
    //依旧是同步阻塞的
    future.get()
    println("over")
  }
}
