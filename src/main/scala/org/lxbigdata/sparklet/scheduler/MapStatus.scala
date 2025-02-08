package org.lxbigdata.sparklet.scheduler

/**
 * ClassName: MapStatus
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: 
 * Result returned by a ShuffleMapTask to a scheduler.
 * Includes the block manager address that
 * the task has shuffle files stored on as well as the sizes of
 * outputs for each reducer, for passing on to the reduce tasks.
 * 在单机版本的实现中这个类作用就很小了
 * @author lx
 * @version 1.0   
 */
case class MapStatus(success:Boolean) {

}
