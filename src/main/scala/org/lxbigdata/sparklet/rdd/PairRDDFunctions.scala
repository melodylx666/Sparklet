package org.lxbigdata.sparklet.rdd

import scala.reflect.ClassTag

/**
 * ClassName: PairRDDFunctions
 * Package: org.lxbigdata.sparklet.rdd
 * Description: RDD增强类，提供pair类型的rdd的常用操作
 *
 * @author lx
 * @version 1.0   
 */
class PairRDDFunctions[K:ClassTag, V:ClassTag]
(
   val self: RDD[(K, V)]
)extends Serializable{
}
