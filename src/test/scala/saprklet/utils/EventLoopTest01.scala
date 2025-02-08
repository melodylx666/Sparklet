package saprklet.utils

import org.junit.jupiter.api.Test
import org.lxbigdata.sparklet.util.EventLoop

import scala.collection.mutable

/**
 * ClassName: EventLoopTest01
 * Package: saprklet.utils
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class EventLoopTest01 {
  class TestEventLoop[E](name:String) extends EventLoop[E](name){
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
  @Test
  def test01(): Unit = {
    val test = new TestEventLoop[String]("test")
    test.start()
    test.post("hello")
    test.post("world")
    test.post("!")
    Thread.sleep(1000)
    assert(test.getMeg == "hello,world,!")
    test.stop()
  }

}
