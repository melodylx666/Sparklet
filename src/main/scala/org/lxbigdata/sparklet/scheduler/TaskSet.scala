package org.lxbigdata.sparklet.scheduler

/**
 * ClassName: TaskSet
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: 一个stage的任务集合
 *
 * @author lx
 * @version 1.0   
 */
class TaskSet
(
  val tasks:Array[Task[_]],
  val stageId:Int,
  val jobId:Int
)