package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.SparkletConf

import java.util.concurrent.{Callable, CompletableFuture, ExecutorService, Executors, Future}


/**
 * ClassName: TaskScheduler
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: 任务调度，此处使用FIFO模式提交到本地线程池
 *
 * @author lx
 * @version 1.0   
 */
class SimpleTaskScheduler{
  //不用trait了反正只实现单机版
  private var _backEnd:LocalBackEnd = null
  private var _dagScheduler:DAGScheduler = null

  def init(backend: LocalBackEnd, dagScheduler:DAGScheduler):Unit = {
    _backEnd = backend
    _dagScheduler = dagScheduler
  }

  def getDagScheduler():DAGScheduler = {
    _dagScheduler
  }

  def submitTasks(taskSet: TaskSet) = {
    val tasks = taskSet.tasks
    _backEnd.receiveOffers(tasks)
  }
}

