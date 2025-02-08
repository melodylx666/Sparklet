package org.lxbigdata.sparklet.shuffle

/**
 * ClassName: ShuffleWriterGroup
 * Package: org.lxbigdata.sparklet.shuffle
 * Description: spark-0.81之后引入的优化，一个cores上的task为一个group,共用桶
 *
 * @author lx
 * @version 1.0   
 */
trait ShuffleWriterGroup {
  val writers: Array[DiskObjectWriter]

  def releaseWriters(success: Boolean)

}
