package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.TaskContext
import org.lxbigdata.sparklet.rdd.RDD

/**
 * ClassName: DAGSchedulerEvent
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: DAGScheduler事件类型
 *
 * @author lx
 * @version 1.0
 */
sealed trait DAGSchedulerEvent

//提交任务事件，这里全部只用这一个
case class SimpleSubmitted
(
  jobId:Int,
  finalRDD:RDD[_],
  func:(TaskContext,Iterator[_]) => _,
  partitions:Array[Int],
  listener:JobListener
)extends DAGSchedulerEvent