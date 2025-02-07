package org.lxbigdata.sparklet

/**
 * ClassName: TaskContext
 * Package: org.lxbigdata.sparklet
 * Description: 每个task的上下文信息,由stage & partition唯一定位
 *
 * @author lx
 * @version 1.0   
 */
class TaskContext
(
  val stageId: Int,
  val partitionId: Int,
){


}
