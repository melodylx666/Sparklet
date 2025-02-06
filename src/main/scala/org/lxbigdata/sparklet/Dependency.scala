package org.lxbigdata.sparklet

import org.lxbigdata.sparklet.rdd.RDD

/**
 * ClassName: Dependency
 * Package: org.lxbigdata.sparklet
 * Description: 依赖关系，分为宽依赖和窄依赖
 *
 * @author lx
 * @version 1.0   
 */
abstract class Dependency [T](val rdd:RDD[T]) extends Serializable

//窄依赖,可以进行算子链合并计算
abstract class NarrowDependency[T](rdd:RDD[T]) extends Dependency[T](rdd) {
  def getParents(partitionId: Int): Seq[Int] //这里是Seq，因为窄依赖分为OneToOne和Range
}

class OneToOneDependency[T](rdd:RDD[T]) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = {
    List(partitionId)
  }
}

class RangeDependency[T](rdd:RDD[T],smallStart:Int,bigStart:Int,length:Int) extends NarrowDependency[T](rdd) {
  override def getParents(partitionId: Int): List[Int] = {
    if(partitionId >= bigStart && partitionId < bigStart + length){
      List(smallStart + partitionId - bigStart) //base + offset
    }else{
      List()
    }
  }
}

//todo 宽依赖




