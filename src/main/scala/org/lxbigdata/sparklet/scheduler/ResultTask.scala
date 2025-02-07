package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.{Partition, TaskContext}
import org.lxbigdata.sparklet.rdd.RDD

/**
 * ClassName: ResultTask
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: result task
 *
 * @author lx
 * @version 1.0   
 */
class ResultTask [T,U]
(
 stageId:Int,
 rdd:RDD[T],
 func:(TaskContext,Iterator[T]) =>U,
 partition:Partition
) extends Task[U] (stageId,partition.index){

  override def runTask(context: TaskContext): U = {
    println("result task run")
    func(context, rdd.iterator(partition, context))
  }
}
