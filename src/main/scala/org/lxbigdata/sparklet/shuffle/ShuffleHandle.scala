package org.lxbigdata.sparklet.shuffle

import org.lxbigdata.sparklet.ShuffleDependency

/**
 * ClassName: ShuffleHandle
 * Package: org.lxbigdata.sparklet.shuffle
 * Description: shuffle handle
 *
 * @author lx
 * @version 1.0   
 */
abstract class ShuffleHandle(val shuffleId:Int)  extends Serializable{
}
// base shuffleHandle类
class BaseShuffleHandle[K,V,C](shuffleId:Int, val numMaps:Int,val dependency:ShuffleDependency[_,_,_]) extends ShuffleHandle(shuffleId) {
  override def toString: String = s"BaseShuffleHandle($shuffleId)"
}
