package org.lxbigdata.sparklet.scheduler

import org.lxbigdata.sparklet.ShuffleDependency
import org.lxbigdata.sparklet.rdd.RDD

/**
 * ClassName: ShuffleMapStage
 * Package: org.lxbigdata.sparklet.scheduler
 * Description: 中间的包含ShuffleWrite的Stage
 *
 * @author lx
 * @version 1.0   
 */
class ShuffleMapStage
(
  id:Int,
  rdd:RDD[_],
  numTasks: Int,
  parents:List[Stage],
  firstJobId:Int,
  val shuffleDep:ShuffleDependency[_,_,_]
) extends Stage(id,rdd,numTasks,parents, firstJobId) {
  //Returns true if the map stage is ready, i.e. all partitions have shuffle outputs
  //todo 第三个大问题,这里没有变导致阻塞
  var isAvailable: Boolean = false


  override def finsMissingPartitions(): Seq[Int] = {
    return (0 until numTasks)
  }
}
