package org.lxbigdata.sparklet

import org.lxbigdata.sparklet.rdd.RDD
import org.lxbigdata.sparklet.serializer.Serializer
import org.lxbigdata.sparklet.util.Aggregator

import scala.reflect.ClassTag

/**
 * ClassName: Dependency
 * Package: org.lxbigdata.sparklet
 * Description: 依赖关系，分为宽依赖和窄依赖
 *
 * @author lx
 * @version 1.0   
 */
abstract class Dependency [T] extends Serializable{
  def rdd:RDD[T]
}

//窄依赖,可以进行算子链合并计算
abstract class NarrowDependency[T](_rdd:RDD[T]) extends Dependency[T] {
  def getParents(partitionId: Int): Seq[Int] //这里是Seq，因为窄依赖分为OneToOne和Range
}

class OneToOneDependency[T](_rdd:RDD[T]) extends NarrowDependency[T](_rdd) {
  override def getParents(partitionId: Int): List[Int] = {
    List(partitionId)
  }

  override def rdd: RDD[T] = _rdd
}

class RangeDependency[T](_rdd:RDD[T],smallStart:Int,bigStart:Int,length:Int) extends NarrowDependency[T](_rdd) {
  override def getParents(partitionId: Int): List[Int] = {
    if(partitionId >= bigStart && partitionId < bigStart + length){
      List(smallStart + partitionId - bigStart) //base + offset
    }else{
      List()
    }
  }

  override def rdd: RDD[T] = _rdd
}

//todo 宽依赖
class ShuffleDependency[K:ClassTag,V:ClassTag,C:ClassTag]
(
  @transient private var _rdd:RDD[_<:Product2[K,V]],
  val partitioner:Partitioner,
  val serializer:Serializer,
  val keyOrdering:Option[Ordering[K]]=None,
  val aggregator:Option[Aggregator[K,V,C]]=None,
  val mapSideCombine:Boolean=false
) extends Dependency[Product2[K,V]]{
  //shuffleId
  val shuffleId =  _rdd.context.newShuffleId()
  //获取shuffle handle来操作shuffle
  val shuffleHandle = _rdd.context.getEnv.shuffleManager.registerShuffle(shuffleId,_rdd.partitions.length,this)
  //rdd
  override def rdd: RDD[Product2[K, V]] = {
    _rdd.asInstanceOf[RDD[Product2[K,V]]]
  }
}



