package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.SparkletConf

import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

/**
 * ClassName: LocalBackend
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: 本地任务执行端,就不用rpc了
 *
 * @author lx
 * @version 1.0   
 */
class LocalBackEnd(conf:SparkletConf, taskScheduler:SimpleTaskScheduler){
  //使用协程/虚拟线程
  private val executors = Executors.newVirtualThreadPerTaskExecutor()
  private class TaskRunner(task:Task[_]) extends Runnable {

    override def run(): Unit = {
      val value: Any = task.run()
      value match {
        case m:MapStatus => {
          //拿到dagScheduler的eventLoop就可以
          //没有listenerBus逻辑，只能暂时这么做了
          handleMapStatus(task,m)
        }
        case r => {
          //todo result task结果
        }
      }
    }
  }
  //todo 暂时这么处理maptask结果
  private def handleMapStatus(task: Task[_],mapStatus:MapStatus):Unit = {
    val scheduler: DAGScheduler = taskScheduler.getDagScheduler()
    scheduler.eventProcessLoop.post(new SimpleCompletion(task, mapStatus))
  }
  def receiveOffers(tasks:Array[Task[_]]):Unit = {
    //todo 这里异步任务的组合逻辑该怎么设计，还是说再加一个listener?
    tasks
      .map(task => new TaskRunner(task))
      .foreach(x => executors.submit(x))
  }

  def shutdown():Unit = {
    println("shutdown local backend")
    executors.shutdown()
  }
}

