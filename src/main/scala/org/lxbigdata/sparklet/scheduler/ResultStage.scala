package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.TaskContext
import org.lxbigdata.sparklet.rdd.RDD

/**
 * ClassName: ResultStage
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: 
 * 最后一个Stage,其TaskSet需要被提交以触发计算，并获取结果
 * @author lx
 * @version 1.0   
 */
class ResultStage
(
  id:Int,
  rdd:RDD[_],
  val func: (TaskContext,Iterator[_]) => _,
  val partitions:Array[Int],
  parents:List[Stage],
  firstJobId:Int
)extends Stage(id,rdd,partitions.length,parents, firstJobId) {

  override def finsMissingPartitions(): Seq[Int] = {
    return (0 until numPartitions)
  }
}
