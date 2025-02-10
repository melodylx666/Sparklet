package org.lxbigdata.sparklet.util

import org.lxbigdata.sparklet.TaskContext

/**
 * ClassName: Aggregator
 * Package: org.lxbigdata.sparklet.util
 * Description:
 *
 * @author lx
 * @version 1.0   
 */
case class Aggregator [K,V,C]
(
  createCombiner:V =>C,
  mergeValue: (C,V) => C,
  mergeCombiner: (C,C) => C
){
  def combineCombinerByKey(iter:Iterator[_<:Product2[K,C]],context:TaskContext):Iterator[(K,C)] = {
    //此处的identity就是 x => x
    val combiners = new ExternalAppendOnlyMap[K,C,C](identity, mergeCombiner,mergeCombiner)
    combiners.insertAll(iter)
    combiners.iterator
  }
  def combineValuesByKey(iter:Iterator[_ <: Product2[K,V]],context:TaskContext):Iterator[(K,C)] = {
    val combiners = new ExternalAppendOnlyMap[K,V,C](createCombiner, mergeValue, mergeCombiner)
    combiners.insertAll(iter)
    combiners.iterator
  }
}
