package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.rdd.RDD

/**
 * ClassName: Stage
 * Package: org.lxbigdata.sparklet.scheduler
 * Description:
 * Stage抽象类，一个Job中的Stage是通过Shuffle来划分的，
 * 而Stage中的算子可以按照分区粒度进行拍扁计算，抽象为task
 *
 * @author lx
 * @version 1.0   
 */
abstract class Stage
(
  val id:Int,
  val rdd:RDD[_],
  val numTasks:Int,
  val parents:List[Stage],
  val firstJobId:Int
){
  val numPartitions:Int = rdd.partitions.size
  //获取未提交的分区
  def finsMissingPartitions():Seq[Int]

}
