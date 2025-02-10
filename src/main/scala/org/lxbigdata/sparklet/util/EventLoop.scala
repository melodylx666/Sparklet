package org.lxbigdata.sparklet.util

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}
import scala.util.control.NonFatal

/**
 * ClassName: EventLoop
 * Package: org.lxbigdata.sparklet.util
 * Description:
 * An event loop to receive events from the caller and process all events in the event thread.
 * It will start an exclusive event thread to process all events.
 *
 * @author lx
 * @version 1.0   
 */
abstract class EventLoop[E](name:String) {
  private val eventQueue:BlockingQueue[E] = new LinkedBlockingDeque[E]()
  private val stopped = new AtomicBoolean(false)
  //后台守护线程，用于 master Scheduler事件分发
  private val eventThread = new Thread(name){
    setDaemon(true)
    setName("loop thread")
    override def run():Unit = {
      try{
        while(!stopped.get()){
          //todo 第二次大问题，queue阻塞
          //todo 第四次，resultStage再次阻塞了
          val event = eventQueue.take() //blocking when queue is empty
          onReceive(event)
        }
      }catch {
        case ie:InterruptedException =>
        case NonFatal(e) => e.printStackTrace()
      }
    }
  }
  //start all
  //todo 第一次全局调试，忘记启动了，全局资源都暂时没有统一关闭
  def start():Unit = {
    if(stopped.get()){
      throw new IllegalStateException(s"${name} has already been stopped")
    }
    onStart()
    eventThread.start()
  }
  //stop all
  def stop():Unit = {
    if(stopped.compareAndSet(false,true)){
      eventThread.interrupt()
      var onStopMethodCalled = false
      try{
        eventThread.join()
        onStopMethodCalled = true
        onStop()
      }catch {
        case ie:InterruptedException => {
          Thread.currentThread().interrupt()
          if(!onStopMethodCalled) onStop()
        }
      }
    }else{
      //do nothing
    }
  }
  //post the event
  def post(event:E):Unit = {
    if(!stopped.get()){
      if(eventThread.isAlive){
        eventQueue.put(event)
      }
    }
  }

  def isActive:Boolean = eventThread.isAlive
  /*-------生命周期方法---------*/
  def onStart():Unit = {}
  def onStop():Unit = {}
  def onReceive(event:E):Unit = {}
}
