package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.util.{EventLoop, ThreadUtils}
import org.lxbigdata.sparklet.{ShuffleDependency, SparkletContext, TaskContext}
import org.lxbigdata.sparklet.rdd.RDD

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable
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

  private def createResultStage
  (
    finalRDD:RDD[_],
    func:Function2[TaskContext,Iterator[_],_],
    partitions:Array[Int],
    jobId:Int
  ): ResultStage = {
    val shuffleDeps = getShuffleDependencies(finalRDD)
    ???
  }

  private def getShuffleDependencies(curRDD: RDD[_]):mutable.Set[ShuffleDependency[_,_,_]] = {
    //用来保存shuffle依赖的hashSet，因为上游可能有多个依赖关系(比如join算子)，所以用set集合
    val parents = mutable.Set[ShuffleDependency[_,_,_]]()
    //已经访问过的RDD
    val visited = mutable.Set[RDD[_]]()
    //等待访问的RDD
    val waitingForVisit = mutable.Stack[RDD[_]]()
    waitingForVisit.push(curRDD)
    while(waitingForVisit.nonEmpty){
      val toVisit = waitingForVisit.pop()
      if(!visited.contains(toVisit)){
        visited += toVisit
        toVisit.dependencies.foreach{
          case shuffleDep:ShuffleDependency[_,_,_] => {
            parents += shuffleDep
          }
          case dependency => {
            waitingForVisit.push(dependency.rdd)
          }
        }
      }
    }
    //如果3个Stage为 A -> B -> C,则返回B->C中的Dep
    parents
  }

}

//the main event loop
//todo ：eventLoop机制文档
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
