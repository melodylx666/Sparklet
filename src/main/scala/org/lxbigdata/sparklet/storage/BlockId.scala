package org.lxbigdata.sparklet.storage

/**
 * ClassName: BlockId
 * Package: org.lxbigdata.sparklet.storage
 * Description: 全局唯一的数据块标识
 * 由一个shuffle中的一个mapId和reduceId组成
 * 最后刷盘的文件名称就是它
 *
 * @author lx
 * @version 1.0   
 */
abstract class BlockId {
  def name:String
}
case class ShuffleBlockId(shuffleId:Int,mapId:Int,reduceId:Int) extends BlockId {
  override def name: String = "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId
}
