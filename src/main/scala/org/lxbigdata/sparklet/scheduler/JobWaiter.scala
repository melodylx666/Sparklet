package org.lxbigdata.sparklet.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}

/**
 * ClassName: JobWaiter
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class JobWaiter[T]
(
  dagScheduler: DAGScheduler,
  val jobId: Int,
  totalTasks: Int,
  resultHandler: (Int, T) => Unit
) extends JobListener{
  JobWaiter =>

  private val finishedTasks = new AtomicInteger(0)
  private val jobPromise:Promise[Unit] =
    if(totalTasks == 0) Promise.successful(()) else Promise[Unit]()

  def jobFinished:Boolean = jobPromise.isCompleted
  //这里的promise其实在分布式情况下是要被远端的任务线程调用rpc填充的，但是本地为了完整性还是用了这个
  def completionFuture: Future[Unit] = jobPromise.future
  /**
   * one resultTask to one this method
   *
   * @param index  对应分区任务
   * @param result 任务结果
   */
  override def taskSucceeded(index: Int, result: Any): Unit = {
    resultHandler(index,result.asInstanceOf[T])
    println("final task succeed:" + Thread.currentThread().getName)
    if(finishedTasks.incrementAndGet() == totalTasks){
      jobPromise.success(())
    }
  }

  override def jobFailed(exception: Exception): Unit = {
    if(!jobPromise.tryFailure(exception)){
      println(s"Ignore failure:${exception.getMessage}")
    }
  }
}
object JobWaiter{
  //拿到全局的JobWaiter
  private val waiters = new ConcurrentHashMap[Int, JobWaiter[_]]()
  def getOrCreate[T](
    dagScheduler: DAGScheduler,
    jobId: Int,
    totalTasks: Int,
    resultHandler: (Int, T) => Unit
  ): JobWaiter[_] = {
    waiters.computeIfAbsent(jobId, _ => new JobWaiter[T](dagScheduler, jobId, totalTasks, resultHandler))
  }

  def checkJob(jobId: Int): JobWaiter[_] = {
    waiters.get(jobId)
  }
}