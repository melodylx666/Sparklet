package org.lxbigdata.sparklet.rdd

import org.lxbigdata.sparklet.serializer.Serializer
import org.lxbigdata.sparklet.util.Aggregator
import org.lxbigdata.sparklet.{HashPartitioner, Partitioner}

import scala.reflect.ClassTag

/**
 * ClassName: PairRDDFunctions
 * Package: org.lxbigdata.sparklet.rdd
 * Description: RDD增强类，提供pair类型的rdd的常用操作
 *
 * @author lx
 * @version 1.0   
 */
class PairRDDFunctions[K:ClassTag,V:ClassTag]
(
  self:RDD[(K,V)]
) extends Serializable {
  def reduceByKey(func: (V, V) => V): RDD[(K, V)] = {
    reduceByKey(new HashPartitioner(self.partitions.length), func)
  }

  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): RDD[(K, V)] = {
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
  }

  def combineByKeyWithClassTag[C: ClassTag]
  (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiner: (C, C) => C,
    partitioner: Partitioner,
    mapSideCombine: Boolean = true,
    serializer: Serializer = null
  ): RDD[(K, C)] = {
    val aggregator = new Aggregator[K, V, C](
      createCombiner,
      mergeValue,
      mergeCombiner
    )
    //可以不shuffle
    if (self.partitioner.contains(partitioner)) {
      //TODO
      null
    } else {
      new ShuffledRDD[K, V, C](self, partitioner)
        .setSerializer(serializer)
        .setAggregator(aggregator)
        .setMapSideCombine(mapSideCombine)
    }
  }
}