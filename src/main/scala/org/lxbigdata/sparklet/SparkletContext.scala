package org.lxbigdata.sparklet

import org.lxbigdata.sparklet.rdd.{FileRDD, RDD}
import org.lxbigdata.sparklet.scheduler.{DAGScheduler, LocalBackEnd, SimpleTaskScheduler}

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.ClassTag

/**
 * ClassName: SparkletContext
 * Package: org.lxbigdata.sparklet
 * Description: Sparklet任务上下文信息
 *
 * @author lx
 * @version 1.0
 */
class SparkletContext(sparkletConf: SparkletConf) {

  private val nextRDDId = new AtomicInteger(0)
  private val nextShuffleId = new AtomicInteger(0)
  private val stopped = new AtomicInteger(0)
  //上下文变量:env,dagScheduler,taskScheduler
  //真正的spark实现中，有30个之多
  private var _env: SparkletEnv = SparkletEnv.createDriverEnv(sparkletConf)
  private var _taskScheduler: SimpleTaskScheduler = new SimpleTaskScheduler()
  private var _dagScheduler: DAGScheduler = new DAGScheduler(this, _taskScheduler)
  //注意这个backend
  private var _backend = {
    val end = new LocalBackEnd(sparkletConf, _taskScheduler)
    end
  }
  //初始化taskScheduler
  //todo
  _taskScheduler.init(_backend, _dagScheduler)
  def newRDDId(): Int = {
    nextRDDId.getAndIncrement()
  }
  def newShuffleId(): Int = {
    nextShuffleId.getAndIncrement()
  }
  def getEnv: SparkletEnv = {
    _env
  }
  /*-------------用户api系列------------*/
  def textFile(path:String):RDD[String] = {
    new FileRDD[String](this,path)
  }
  /*-------------提交任务系列------------*/
  //rdd,func
  def runJob[T:ClassTag, U:ClassTag](finalRDD: RDD[T],func:Iterator[T] => U):Array[U] = {
    runJob(finalRDD,func,finalRDD.partitions.indices)
  }
  //rdd,func,partitions
  def runJob[T,U:ClassTag]
  (
    finalRDD:RDD[T],
    func:Iterator[T] => U,
    partitions:Seq[Int]
  ):Array[U] = {
    runJob[T,U](finalRDD,(_:TaskContext,iter:Iterator[T]) => func(iter),partitions)
  }

  //RDD,func,partitions,taskContext
  def runJob[T,U:ClassTag]
  (
    finalRDD:RDD[T],
    func:(TaskContext,Iterator[T]) => U,
    partitions:Seq[Int]
  ):Array[U] = {
    //创建一个数组，用于存储从本地/远端收集的每个分区结算的的结果
    val results = new Array[U](partitions.size)
    runJob[T,U](finalRDD,func,partitions,(index,res) => results(index) = res)
    results
  }

  //RDD,func,partitions,resultHandler，无返回值，但是有闭包所以可以本地拿到结果
  def runJob[T,U:ClassTag]
  (
    finalRDD:RDD[T],
    func:(TaskContext,Iterator[T]) => U,
    partitions:Seq[Int],
    resultHandler:(Int,U) => Unit
  ):Unit = {
    _dagScheduler.runJob(finalRDD,func,partitions.toArray,resultHandler)
  }
}
