package org.lxbigdata.sparklet.scheduler

import java.util.concurrent.{Callable, CompletableFuture, ExecutorService, Executors, Future}


/**
 * ClassName: TaskScheduler
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: 任务调，此处使用FIFO模式提交到本地线程池
 *
 * @author lx
 * @version 1.0   
 */
class SimpleTaskScheduler{
  private val executors = Executors.newVirtualThreadPerTaskExecutor()
  def getExecutors: ExecutorService = executors

  def submitTasks(taskSet: TaskSet): Array[Future[Any]] = {
    val tasks = taskSet.tasks
    return tasks.map(task => {
      executors.submit(() => {
        task.run()
      })
    })
  }
}