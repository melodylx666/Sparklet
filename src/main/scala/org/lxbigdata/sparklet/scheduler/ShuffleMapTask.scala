package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.{Partition, ShuffleDependency, TaskContext}
import org.lxbigdata.sparklet.rdd.RDD
import org.lxbigdata.sparklet.shuffle.{HashShuffleManager, ShuffleHandle, ShuffleManager}

/**
 * ClassName: ShuffleMapTask
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: for(task <- Stage.tasks) yield shuffleMapTask(task)
 *
 * @author lx
 * @version 1.0   
 */
class ShuffleMapTask
(
  stageId:Int,
  rdd:RDD[_],
  dep:ShuffleDependency[_,_,_],
  partition:Partition
) extends Task[MapStatus](stageId,partition.index){

  override def runTask(context: TaskContext): MapStatus = {
    println("shuffleMapTask开始执行")
    val manager = new HashShuffleManager()
    val writer = manager.getWriter[Any,Any](dep.shuffleHandle, partitionId, context)
    writer.write(rdd.iterator(partition, context).asInstanceOf[Iterator[_ <: Product2[Any,Any]]])
    return writer.stop(success = true).get
  }
}
