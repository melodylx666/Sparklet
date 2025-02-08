package saprklet.utils

import org.junit.jupiter.api.Test
import org.lxbigdata.sparklet.util.{EventLoop, ThreadUtils}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
import scala.concurrent.Promise
import scala.concurrent.duration.Duration

/**
 * ClassName: EventLoopTest01
 * Package: saprklet.utils
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class EventLoopTest01 {
  class TestEventLoop01[E](name:String) extends EventLoop[E](name){
    private val megList = mutable.ListBuffer[E]()
    def getMeg: String = megList.mkString(",")

    override def onStart(): Unit = {
      println("my loop start")
    }
    override def onReceive(event: E): Unit = {
      megList += event
    }
    override def onStop(): Unit = {
      println("my loop stop")
    }
  }
  class TestEventLoop02[E](name:String,totalEvent:Int) extends EventLoop[E](name){
    private val megList = mutable.ListBuffer[E]()
    private val promise = Promise[Unit]()
    private var eventCnt = new AtomicInteger(0)

    def getMeg: String = megList.mkString(",")
    def getPromise:Promise[Unit] = promise

    override def onStart(): Unit = {
      println("my loop start")
    }
    override def onReceive(event: E): Unit = {
      Thread.sleep(3000)
      megList += event
      if(eventCnt.incrementAndGet() == totalEvent){
        promise.success(())
      }
    }
    override def onStop(): Unit = {
      println("my loop stop")
    }
  }
  class TestEventLoop03[E](name:String,totalEvent:Int) extends EventLoop[E](name){
    private val megList = mutable.ListBuffer[E]()
    private val promise = Promise[Unit]()
    private var eventCnt = new AtomicInteger(0)

    def getMeg: String = megList.mkString(",")
    def getPromise:Promise[Unit] = promise

    override def onStart(): Unit = {
      println("my loop start")
    }
    override def onReceive(event: E): Unit = {
      Thread.ofVirtual().start(() => {
        Thread.sleep(3000)
        megList += event
        if(eventCnt.incrementAndGet() == totalEvent){
          promise.success(())
        }
      })
    }
    override def onStop(): Unit = {
      println("my loop stop")
    }
  }

  @Test
  def test01(): Unit = {
    val test = new TestEventLoop01[String]("test")
    test.start()
    test.post("hello")
    test.post("world")
    test.post("!")
    Thread.sleep(1000)
    assert(test.getMeg == "hello,world,!")
    test.stop()
  }
  @Test
  def test02(): Unit = {
    //这里eventLoop只有单线程逻辑，阻塞情况下,性能很差,结果为9秒
    val test = new TestEventLoop02[String]("test",3)
    test.start()
    val startTime = System.currentTimeMillis()
    test.post("hello")
    test.post("world")
    test.post("!")
    val promiseList = test.getPromise
    ThreadUtils.awaitReady(promiseList.future,Duration.Inf)
    val endTime = System.currentTimeMillis()
    println(s"cost time with single-thread:${(endTime-startTime)/1000}")
    println("meg:"+test.getMeg)
    test.stop()
  }
  @Test
  def test03(): Unit = {
    //暂时在实现中将任务放在该主线程的虚拟线程中执行，只需要3秒钟实现
    val test = new TestEventLoop03[String]("test",3)
    test.start()
    val startTime = System.currentTimeMillis()
    test.post("hello")
    test.post("world")
    test.post("!")
    val promiseList = test.getPromise
    ThreadUtils.awaitReady(promiseList.future,Duration.Inf)
    val endTime = System.currentTimeMillis()
    println(s"cost time with single-thread:${(endTime-startTime)/1000}")
    println("meg:"+test.getMeg)
    test.stop()
  }

}
