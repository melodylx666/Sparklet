package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.{Partition, TaskContext}
import org.lxbigdata.sparklet.rdd.RDD

import java.util.logging.{ConsoleHandler, Logger}

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
  private val logger = Logger.getLogger(s"this.getClass.getName-${stageId}-${partition.index}")
  logger.addHandler(new ConsoleHandler())
  logger.setUseParentHandlers(false)
  logger.setLevel(java.util.logging.Level.INFO)

  override def runTask(context: TaskContext): U = {
    logger.info("result task run")
    func(context, rdd.iterator(partition, context))
  }
}
