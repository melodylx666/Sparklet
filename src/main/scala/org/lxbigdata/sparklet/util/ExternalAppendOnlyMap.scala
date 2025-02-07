package org.lxbigdata.sparklet.util

import org.lxbigdata.sparklet.SparkletEnv
import org.lxbigdata.sparklet.serializer.Serializer
import org.lxbigdata.sparklet.storage.BlockManager

import scala.collection.mutable

/**
 * ClassName: ExternalAppendOnlyMap
 * Package: org.lxbigdata.sparklet.util
 * Description: 这里并没有实现External功能，只是简单模拟了对键做两种逻辑的路由，
 *  1.如果key已经存在，则调用mergeValue方法，合并值
 *  2.如果key不存在，则调用createCombiner方法，创建一个值
 *
 * @author lx
 * @version 1.0   
 */
class ExternalAppendOnlyMap[K,V,C]
(
  createCombiner: V => C,
  mergeValue: (C, V) => C,
  mergeCombiners: (C, C) => C,
  serializer:Serializer = SparkletEnv.get.serializer,
  blockManager: BlockManager = SparkletEnv.get.blockManager
) extends Serializable with Iterable[(K, C)]{
  //内部Map
  val currentMap = mutable.HashMap[K, C]()
  override def iterator: Iterator[(K, C)] = {
    currentMap.iterator
  }
  /*-----增强方法-------*/
  def insertAll(entries: Iterator[Product2[K,V]]):Unit = {
    if (currentMap == null) {
      throw new IllegalStateException(
        "Cannot insert new elements into a map after calling iterator")
    }

    var curEntry:Product2[K,V] = null
    while(entries.hasNext){
      curEntry = entries.next()
      val op = currentMap.get(curEntry._1)
      op match {
        case Some(value) => {
          currentMap.put(curEntry._1, mergeValue(value, curEntry._2))
        }
        case None => {
          currentMap.put(curEntry._1, createCombiner(curEntry._2))
        }
      }
    }
  }
  def insertAll(entries: Iterable[Product2[K, V]]): Unit = {
    insertAll(entries.iterator)
  }
  def insert(key: K, value: V): Unit = {
    insertAll(Iterator((key, value)))
  }

}
