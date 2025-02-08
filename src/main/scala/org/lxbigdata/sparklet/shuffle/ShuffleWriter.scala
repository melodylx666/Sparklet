package org.lxbigdata.sparklet.shuffle

import org.lxbigdata.sparklet.scheduler.MapStatus

/**
 * ClassName: ShuffleWriter
 * Package: org.lxbigdata.sparklet.shuffle
 * Description: Shuffle写接口
 *
 * @author lx
 * @version 1.0   
 */
trait ShuffleWriter[K,V] {
  //do shuffle write
  def writer(records:Iterator[_<:Product2[K,V]]):Unit

  //stop shuffle writer and return status
  def stop(success:Boolean):Option[MapStatus]
}
