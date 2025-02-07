package org.lxbigdata.sparklet.rdd

import org.lxbigdata.sparklet.{Partition, TaskContext}

import scala.reflect.ClassTag

/**
 * ClassName: MapPartitionsRDD
 * Package: org.lxbigdata.sparklet.rdd
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class MapPartitionsRDD [U:ClassTag, T:ClassTag]
(
   var prev:RDD[T], //上游rdd
   var f:(TaskContext,Int,Iterator[T]) => Iterator[U]
) extends RDD[U](prev){

  //返回分区数组,就是上游rdd的分区数组
  override def getPartitions: Array[Partition] = {
    firstParent[T].partitions
  }
  //计算逻辑
  override def compute(split: Partition, context: TaskContext): Iterator[U] = {
    //传入上一个rdd的计算迭代器，并在此再包装一层计算逻辑。
    f(context,split.index,firstParent[T].iterator(split,context))
  }
}
