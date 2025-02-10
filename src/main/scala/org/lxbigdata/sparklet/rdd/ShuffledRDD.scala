package org.lxbigdata.sparklet.rdd

import org.lxbigdata.sparklet.serializer.Serializer
import org.lxbigdata.sparklet.util.Aggregator
import org.lxbigdata.sparklet.{Dependency, Partition, Partitioner, ShuffleDependency, SparkletEnv, TaskContext}

import scala.reflect.ClassTag

/**
 * ClassName: ShuffledRDD
 * Package: org.lxbigdata.sparklet.rdd
 * Description: ShuffledRDD
 *
 * @author lx
 * @version 1.0   
 */
class ShuffledRDDPartition(val i:Int) extends Partition{
  override def index: Int = i
}

class ShuffledRDD[K:ClassTag,V:ClassTag,C:ClassTag]
( //上游RDD内数据必须是Product2及其子类型，比如Tuple2，还有Spark内部定义的:MutablePair
  @transient var prev:RDD[_<:Product2[K,V]],
  part:Partitioner
) extends RDD[(K,C)](prev.context,Nil){

  private var userSpecifiedSerializer:Option[Serializer] = None
  private var serializer:Serializer = userSpecifiedSerializer.getOrElse(SparkletEnv.get.serializer)
  private var aggregator:Option[Aggregator[K,V,C]] = None
  private var mapSideCombine:Boolean = false
  private var keyOrdering:Option[Ordering[K]] = None

  override def getPartitions: Array[Partition] = {
    //创建一个ShuffledRDDPartition数组
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
  }

  override def getDependencies: Seq[Dependency[_]] = {
    List(new ShuffleDependency(prev,part,serializer,keyOrdering,aggregator,mapSideCombine))
  }

  //这里是shuffledRDD,需要完成shuffleRead的功能
  override def compute(split: Partition, context: TaskContext): Iterator[(K, C)] = {
    //对于上游是一个RDD的(mapPartitionRDD,ShuffledRDD)，这里返回的List里面只有一个dependency
    //CoGroupRDD的依赖是多个RDD
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[K,V,C]]
    SparkletEnv.get.shuffleManager
      .getReader(dep.shuffleHandle,split.index,split.index+1,context)
      .read() //todo 这里阻塞了
      .asInstanceOf[Iterator[(K,C)]]
  }

  //设置序列化器
  def setSerializer(serializer:Serializer):ShuffledRDD[K,V,C] = {
    //用Option包装，null -> None; x -> Some(x)
    this.userSpecifiedSerializer = Option(serializer)
    this
  }

  //设置聚合器
  def setAggregator(aggregator:Aggregator[K,V,C]):ShuffledRDD[K,V,C] = {
    this.aggregator = Option(aggregator)
    this
  }

  //设置是否在Map端进行聚合
  def setMapSideCombine(mapSideCombine:Boolean):ShuffledRDD[K,V,C] = {
    this.mapSideCombine = mapSideCombine
    this
  }
}
