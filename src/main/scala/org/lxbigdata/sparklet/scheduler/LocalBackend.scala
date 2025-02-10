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
  private class TaskRunner(task:Task[_],jobId:Int) extends Runnable {

    override def run(): Unit = {
      val value: Any = task.run()
      value match {
        case m:MapStatus => {
          //todo 没有listenerBus逻辑，只能暂时这么做了
          MapStageWaiter.checkStage(task.stageId).taskSucceeded(task.partitionId, value)
        }
        case r => {
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

