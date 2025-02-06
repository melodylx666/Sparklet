package org.lxbigdata.sparklet

/**
 * ClassName: Partition
 * Package: org.lxbigdata.sparklet
 * Description: 分区标识
 *
 * @author lx
 * @version 1.0   
 */
trait Partition extends Serializable {

  def index: Int
  override def hashCode(): Int = index

  override def equals(obj: Any): Boolean = super.equals(obj)
}
