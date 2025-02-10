package org.lxbigdata.sparklet.scheduler

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}

/**
 * ClassName: MapStageWaiter
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: 监听某个stage是否完成,利用mapStatus
 *
 * @author lx
 * @version 1.0   
 */
class MapStageWaiter
(
  dagScheduler: DAGScheduler,
  val stageId: Int,
  val numTasks: Int
)extends JobListener {
  stageWaiter =>

  private val finishedTasks = new AtomicInteger(0)
  private val jobPromise:Promise[Unit] = {
    if(numTasks == 0) Promise.successful(()) else Promise[Unit]()
  }
  private val stageFinished = jobPromise.isCompleted

  def completionFuture: Future[Unit] = jobPromise.future
  override def taskSucceeded(index: Int, result: Any): Unit = {
    if(finishedTasks.incrementAndGet() == numTasks){
      jobPromise.success(())
    }
  }

  override def jobFailed(exception: Exception): Unit = {
    if(!jobPromise.tryFailure(exception)){
      println(s"Ignore failure:${exception.getMessage}")
    }
  }
}
object MapStageWaiter{
  //拿到全局的mapStageWaiters
  private val mapStageWaiters = new ConcurrentHashMap[Int,MapStageWaiter]()
  def getOrCreate(dagScheduler: DAGScheduler,stageId:Int,numTasks:Int):MapStageWaiter = {
    mapStageWaiters.computeIfAbsent(stageId,_ => new MapStageWaiter(dagScheduler,stageId,numTasks))
  }

  def checkStage(stageId: Int): MapStageWaiter = {
    mapStageWaiters.get(stageId)
  }

}
