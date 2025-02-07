package org.lxbigdata.sparklet

/**
 * ClassName: Partitioner
 * Package: org.lxbigdata.sparklet
 * Description: 分区器，以及哈希分区器实现
 *
 * @author lx
 * @version 1.0   
 */
abstract class Partitioner {
  def numPartitions: Int
  def getPartition(key: Any): Int
}

class HashPartitioner(partitions: Int) extends Partitioner {
  //获取分区总数
  override def numPartitions: Int = partitions
  //计算key的分区
  override def getPartition(key: Any): Int = key match {
    case x if x != null => {
      val k = x.hashCode()
      val rawMod = k % partitions
      rawMod +(if (rawMod < 0) numPartitions  else 0)
    }
    case _ => 0 //这里null的分区全部是0,也是数据倾斜的来源之一
  }
  //判断分区器是否相等，两个分区器相等，当且仅当分区总数相等
  override def equals(obj: Any): Boolean = {
    obj match {
      case hp: HashPartitioner => hp.numPartitions == numPartitions
      case _ => false
    }
  }
}
