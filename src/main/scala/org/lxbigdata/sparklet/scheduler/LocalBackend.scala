package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.SparkletConf

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, ExecutorService, Executors, Future, ThreadFactory}

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
//  private val executors = Executors.newVirtualThreadPerTaskExecutor()
  private val executors: ExecutorService = Executors.newFixedThreadPool(4,new CustomThreadFactory("local-backend-thread"));
  private class TaskRunner(task:Task[_],jobId:Int) extends Runnable {

    override def run(): Unit = {
      val value: Any = task.run()
      value match {
        case m:MapStatus => {
          //todo 没有listenerBus逻辑，只能暂时这么做了
          MapStageWaiter.checkStage(task.stageId).taskSucceeded(task.partitionId, value)
        }
        case _ => {
          println(s"Thread:${Thread.currentThread().getName}")
          JobWaiter.checkJob(jobId).taskSucceeded(task.partitionId, value)
        }
      }
    }
  }

  def receiveOffers(taskSet: TaskSet):Unit = {
    val tasks = taskSet.tasks
    val jobId = taskSet.jobId
    tasks
      .map(task => new TaskRunner(task, jobId))
      .foreach(x => executors.submit(x))
  }

  def shutdown():Unit = {
    println("shutdown local backend")
    executors.shutdown()
  }
}
class CustomThreadFactory(name:String) extends ThreadFactory{
  private val threadCount = new AtomicInteger(0)
  override def newThread(r: Runnable): Thread = {
    val thread = new Thread(r)
    thread.setName(s"${name}-${threadCount.getAndIncrement()}")
    thread
  }
}

