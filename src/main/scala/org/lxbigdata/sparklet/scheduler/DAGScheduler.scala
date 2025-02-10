package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.util.{EventLoop, ThreadUtils}
import org.lxbigdata.sparklet.{NarrowDependency, Partition, ShuffleDependency, SparkletContext, TaskContext}
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
  private val nextStageId = new AtomicInteger(0)
  //todo 暂时暴露出来让backend使用
  val eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  //shuffleId -> ShuffleMapStage
  private val shuffleIdToMapStage = mutable.Map[Int,ShuffleMapStage]()
  //stageId -> stage
  private val stageIdToStage = mutable.Map[Int,Stage]()

  //Stages we need to run whose parents aren't done
  private val waitingStages = mutable.Set[Stage]()
  //Stages we are running right now
  private val runningStages = mutable.Set[Stage]()

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
//    val waiter = new JobWaiter[U](this,jobId,partitions.size,resultHandler)
    val waiter = JobWaiter.getOrCreate[U](this,jobId,partitions.size,resultHandler).asInstanceOf[JobWaiter[U]]
    val fun2 = func.asInstanceOf[(TaskContext,Iterator[_]) => _]
    eventProcessLoop.post(SimpleSubmitted(jobId,finalRDD,fun2,partitions,waiter))
    waiter
  }
  def exec
  (
    jobId: Int,
    finalRdd: RDD[_],
    func: Function2[TaskContext, Iterator[_], _],
    partitions: Array[Int],
    listener: JobListener
  ): Unit = {
    //create all stage
    val finalStage = createResultStage(finalRdd,func,partitions,jobId)
    //todo listener没有用上
    //submit stage
    submitStage(finalStage)
  }

  /*-------stage create methods系列，-------*/
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
    //todo stack or ListBuffer?
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

  private def getOrCreateParentStage(shuffleDeps:mutable.Set[ShuffleDependency[_,_,_]],firstJobId:Int):List[Stage] = {
    shuffleDeps.map{
      case shuffleDep => getOrCreateShuffleMapStage(shuffleDep,firstJobId)
    }.toList
  }

  //查找上游依赖的所有shuffleMapStage是否创建，没有则创建
  //之后注册当前shuffleDep
  private def getOrCreateShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], firstJobId: Int):ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match{
      case Some(stage) => stage
      case None => {
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach{
          dep => {
            if(!shuffleIdToMapStage.contains(dep.shuffleId)){
              createShuffleMapStage(dep,firstJobId)
            }
          }
        }
        //上游全部注册完成，则该ShuffleDependency也可以注册了，并创建对应stage
        createShuffleMapStage(shuffleDep,firstJobId)
      }
    }
  }

  //创建shuffleMapStage
  private def createShuffleMapStage[K,V,C](shuffleDep: ShuffleDependency[K, V, C], jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val numTasks = rdd.partitions.length
    //上一层的所有上游stage
    val parents = getOrCreateParentStage(getShuffleDependencies(rdd),jobId)
    val id = nextStageId.getAndIncrement()

    val stage = new ShuffleMapStage(id, rdd, numTasks, parents, jobId, shuffleDep)
    stageIdToStage += (id -> stage)
    shuffleIdToMapStage += (shuffleDep.shuffleId -> stage)
    stage
  }

  //上游所有层没有注册shuffleMapStage的ShuffleDependency
  private def getMissingAncestorShuffleDependencies(rdd:RDD[_]):mutable.ListBuffer[ShuffleDependency[_,_,_]] = {
    val ancestors = mutable.ListBuffer[ShuffleDependency[_,_,_]]()
    val visited = mutable.Set[RDD[_]]()
    val waitingForVisit = mutable.Stack[RDD[_]]()
    waitingForVisit.push(rdd)
    while(waitingForVisit.nonEmpty){
      val toVisit = waitingForVisit.pop()
      if(!visited.contains(toVisit)){
        visited += toVisit
        //上一层的shuffleDep
        val shuffleDeps = getShuffleDependencies(toVisit)
        shuffleDeps.foreach{
          case shuffleDep => {
            //还未创建shuffleMapStage
            if(!shuffleIdToMapStage.contains(shuffleDep.shuffleId)){
              ancestors.prepend(shuffleDep) //最后的从左往右遍历顺序一致，而不是反着
              waitingForVisit.push(shuffleDep.rdd)
            }
          }
        }
      }
    }
    ancestors
  }


  /*------submit stage methods系列-------*/
  private def submitStage(stage:Stage):Unit = {

    if(!waitingStages.contains(stage) && !runningStages.contains(stage)){
      val missing:List[Stage] = getMissingParentsStages(stage).sortBy(x => x.id)
      //base case
      if(missing.isEmpty){
        return submitMissingTasks(stage,nextJobId.getAndIncrement())
      }
      waitingStages +=stage
      missing.foreach(x => submitStage(x))
    }
  }

  private def getMissingParentsStages(stage: Stage):List[Stage] = {
    val missing = mutable.Set[Stage]()
    val visited = mutable.Set[RDD[_]]()
    val waitingForVisit = mutable.Stack[RDD[_]]()
    waitingForVisit.push(stage.rdd)

    while(waitingForVisit.nonEmpty){
      val cur = waitingForVisit.pop()
      if(!visited.contains(cur)){
        visited += cur
        for(dep <- cur.dependencies){
          dep match {
            case shuffleDep:ShuffleDependency[_,_,_] => {
              val shuffleMapStage: ShuffleMapStage = getOrCreateShuffleMapStage(shuffleDep, stage.firstJobId)
              //如果shuffleMapStage没有完成outputs，则加入missing,不必往前了
              if(!shuffleMapStage.isAvailable){
                missing += shuffleMapStage
              }
            }
            case narrowDep:NarrowDependency[_] => {
              waitingForVisit.push(dep.rdd)
            }
          }
        }
      }
    }
    missing.toList
  }

  //提交任务给taskScheduler，让其分发计算任务到backend(remote or local)
  private def submitMissingTasks(stage: Stage,jobId: Int):Unit = {

    runningStages += stage
    //要计算的分区(由于容错机制可能保存过部分分区的输出，所以可能是部分分区)，以及全部的分区信息
    val partitionsToCompute = stage.finsMissingPartitions()
    val partitions: Array[Partition] = stage.rdd.partitions
    //taskSet
    val tasks = stage match {
      case stage: ShuffleMapStage => {
        //todo:更换日志信息输出，不要用println
        println(s"提交ShuffleMapStage")
        stage.isAvailable
        partitionsToCompute.map(id => {
          val partition = partitions(id)
          new ShuffleMapTask(stage.id, stage.rdd, stage.shuffleDep, partition)
        })
      }
      case stage: ResultStage => {
        println(s"提交ResultStage")
        partitionsToCompute.map(id => {
          val partition = partitions(id)
          new ResultTask(stage.id, stage.rdd, stage.func, partition)
        })
      }
    }

    //提交任务给taskScheduler，让其分发计算任务到backend(remote or local)
    if(tasks.nonEmpty){
      //注册一个StageWaiter
      val waiter: MapStageWaiter = MapStageWaiter.getOrCreate(this, stage.id, tasks.size)
      taskScheduler.submitTasks(new TaskSet(tasks.toArray,stage.id,jobId))
      //等待任务完成
      ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf)
      waiter.completionFuture.value.get match {
        case Success(v) => {
          submitWaitingChildStages(stage)
        }
        case Failure(e) => {
          println(s"can not forward")
        }
      }
    }
  }

  //submit the waiting child stage,like: ResultStage
  private def submitWaitingChildStages(stage: Stage):Unit ={
    val stages: Seq[Stage] = waitingStages.filter(x => x.parents.contains(stage)).toList
    waitingStages --= stages
    for(stage <- stages.sortBy(x => x.id)){
      submitStage(stage)
    }
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
