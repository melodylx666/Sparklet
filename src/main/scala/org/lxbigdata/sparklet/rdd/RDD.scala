package org.lxbigdata.sparklet.rdd

import org.lxbigdata.sparklet.{Dependency, OneToOneDependency, Partition, SparkletContext}

import scala.reflect.ClassTag

/**
 * ClassName: RDD
 * Package: org.lxbigdata.sparklet.rdd
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
abstract class RDD[T:ClassTag]
(
  @transient private var sc: SparkletContext,
  @transient private var deps:Seq[Dependency[_]]
) extends Serializable {
  private var partitions_ :Array[Partition] = _
  private var dependencies_ :Seq[Dependency[_]] = _

  /**
   * 辅助构造器，针对窄依赖情况
   * @param oneParent
   */
  def this(@transient oneParent:RDD[_]) = {
    this(oneParent.sc,List(new OneToOneDependency(oneParent)))
  }

  def context: SparkletContext = sc
  //获取该rdd的id
  def id: Int = sc.newRDDId()

  /* 依赖相关 */
  def getDependencies: Seq[Dependency[_]] = deps
  final def dependencies: Seq[Dependency[_]] = {
    if (dependencies_ == null) {
      dependencies_ = getDependencies
    }
    dependencies_
  }
  def firstParent[U:ClassTag]:RDD[U] = {
    dependencies.head.rdd.asInstanceOf[RDD[U]]
  }
  /* 分区相关  */


}
