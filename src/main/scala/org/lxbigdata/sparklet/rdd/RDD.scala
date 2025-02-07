package org.lxbigdata.sparklet.rdd

import org.lxbigdata.sparklet.{Dependency, OneToOneDependency, Partition, Partitioner, SparkletContext, TaskContext}

import scala.reflect.ClassTag

/**
 * ClassName: RDD
 * Package: org.lxbigdata.sparklet.rdd
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
abstract class RDD[T:ClassTag]
(
  @transient private var sc: SparkletContext,
  @transient private var deps:Seq[Dependency[_]]
) extends Serializable {
  //该rdd的分区信息
  private var partitions_ :Array[Partition] = _
  //该rdd的上游依赖信息
  private var dependencies_ :Seq[Dependency[_]] = _

  /**
   * 辅助构造器，针对窄依赖情况
   * @param oneParent
   */
  def this(@transient oneParent:RDD[_]) = {
    this(oneParent.sc,List(new OneToOneDependency(oneParent)))
  }

  def context: SparkletContext = sc
  //获取该rdd的id
  def id: Int = sc.newRDDId()

  /*-------- 依赖相关 ---------*/
  def getDependencies: Seq[Dependency[_]] = deps
  final def dependencies: Seq[Dependency[_]] = {
    if (dependencies_ == null) {
      dependencies_ = getDependencies
    }
    dependencies_
  }
  def firstParent[U:ClassTag]:RDD[U] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }

  /* -------分区相关---------- */
  def partitioner: Option[Partitioner] = None
  //获取该rdd全部分区，由子类实现
  def getPartitions:Array[Partition]
  final def partitions:Array[Partition] = {
    if (partitions_ == null) {
      partitions_ = getPartitions
    }
    partitions_
  }

  /*-------数据计算迭代器-------*/
  final def iterator(split:Partition,context: TaskContext):Iterator[T] = {
    compute(split,context)
  }
  //包装的计算逻辑，由子类实现
  def compute(split:Partition,context: TaskContext):Iterator[T]

  /*--------常用方法:transform & action-----------*/
  //transform算子
  def map[U:ClassTag](f:T => U):RDD[U] = {
    new MapPartitionsRDD[U,T](this,(_,_,iter)=>iter.map(f))
  }

  def flatMap[U:ClassTag](f:T => TraversableOnce[U]):RDD[U] = {
    new MapPartitionsRDD[U,T](this,(_,_,iter)=>iter.flatMap(f))
  }

  def filter(f:T => Boolean):RDD[T] = {
    new MapPartitionsRDD[T,T](this,(_,_,iter)=>iter.filter(f))
  }
  //action算子
  def collect():Array[T] = {
    val results: Array[Array[T]] = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    //打平成一个Array
    Array.concat(results:_*)
  }
  def foreach(f:T => Unit):Unit = {
    sc.runJob(this, (iter: Iterator[T]) => iter.foreach(f))
  }
}

object RDD {
  //隐式-类型转换
  implicit def rddToPairRDDFunctions[K:ClassTag,V:ClassTag](rdd:RDD[(K,V)]):PairRDDFunctions[K,V] = {
    new PairRDDFunctions[K,V](rdd)
  }
}