package org.lxbigdata.sparklet

import org.lxbigdata.sparklet.rdd.RDD

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

  private val env: SparkletEnv = SparkletEnv.createDriverEnv(sparkletConf)


  def newRDDId(): Int = {
    nextRDDId.getAndIncrement()
  }
  def newShuffleId(): Int = {
    nextShuffleId.getAndIncrement()
  }
  def getEnv: SparkletEnv = {
    env
  }

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
      ???
  }
}
