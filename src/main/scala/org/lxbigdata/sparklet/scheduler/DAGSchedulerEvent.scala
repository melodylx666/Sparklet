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

//提交全局Job
case class SimpleSubmitted
(
  jobId:Int,
  finalRDD:RDD[_],
  func:(TaskContext,Iterator[_]) => _,
  partitions:Array[Int],
  listener:JobListener
)extends DAGSchedulerEvent

//MapTask完成的事件
case class SimpleCompletion
(
  task:Task[_],
  result:Any
) extends DAGSchedulerEvent

//最后一个stage的Task完成的事件
case class SimpleFinalTaskCompletion
(
  task: Task[_],
  result:Any
) extends DAGSchedulerEvent

case class SimpleMapStageCompletion
(
  stage:Stage
) extends DAGSchedulerEvent