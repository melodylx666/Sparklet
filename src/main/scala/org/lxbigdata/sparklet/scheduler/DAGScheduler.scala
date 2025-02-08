package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.util.{EventLoop, ThreadUtils}
import org.lxbigdata.sparklet.{SparkletContext, TaskContext}
import org.lxbigdata.sparklet.rdd.RDD

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
 * ClassName: DAGScheduler
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class DAGScheduler
(
  private val sc:SparkletContext,
  private val taskScheduler:SimpleTaskScheduler
){
  private val nextJobId = new AtomicInteger(0)
  private val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  def runJob[T,U]
  (
    finalRDD:RDD[T],
    func:(TaskContext,Iterator[T]) => U,
    partitions:Array[Int],
    resultHandler:(Int,U) => Unit
  ):Unit = {
    val waiter = submitJob(finalRDD,func,partitions,resultHandler)
    ThreadUtils.awaitReady(waiter.completionFuture,Duration.Inf)
    waiter.completionFuture.value.get match {
      case Success(value) => {
        println("job 执行成功了")
      }
      case Failure(exception) => {
        println(s"job 执行失败了:${exception}")
      }
    }
  }

  //submit the job to the loop and then to worker
  def submitJob[T,U]
  (
    finalRDD:RDD[T],
    func:(TaskContext,Iterator[T]) => U,
    partitions:Array[Int],
    resultHandler:(Int,U) => Unit
  ):JobWaiter[U] = {
    val jobId = nextJobId.getAndIncrement()
    val waiter = new JobWaiter[U](this,jobId,partitions.size,resultHandler)

    val fun2 = func.asInstanceOf[(TaskContext,Iterator[_]) => _]
    eventProcessLoop.post(SimpleSubmitted(jobId,finalRDD,fun2,partitions,waiter))
    waiter
  }
  //todo 切分stage提交tasks
  def exec
  (
    jobId: Int,
   rdd: RDD[_],
    func: Function2[TaskContext, Iterator[_], _],
    partitions: Array[Int],
    listener: JobListener
  ): Unit = {

  }



}

//the main event loop
class DAGSchedulerEventProcessLoop(dagScheduler:DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") {
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    doOnReceive(event)
  }
  private def doOnReceive(event:DAGSchedulerEvent):Unit = {
    event match {
      case SimpleSubmitted(jobId,rdd,func,partitions,listener) => {
        dagScheduler.exec(jobId,rdd,func,partitions,listener)
      }
      case _ => {
        println("other event")
      }
    }
  }
}
