package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.TaskContext

/**
 * ClassName: Task
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: 真正的任务包装，一个stage一个分区 => 一个task
 *  有两种类型:ShuffleMapTask，以及ResultTask
 *  A ResultTask executes the task and sends the task output back to the driver application.
 *  A ShuffleMapTask executes the task and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @author lx
 * @version 1.0   
 */
abstract class Task[T]
(
  val stageId:Int,
  val partitionId:Int
)extends Serializable {
  //执行任务
  def runTask(context:TaskContext):T

  def run():T = {
    runTask(new TaskContext(stageId, partitionId))
  }
}


