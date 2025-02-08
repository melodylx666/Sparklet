package org.lxbigdata.sparklet.scheduler

/**
 * ClassName: JobListener
 * Package: org.lxbigdata.sparklet.scheduler
 * Description:
 * the listener is notified each time a task succeeds, as well as if
 * the whole job fails (and no further taskSucceeded events will happen).
 *
 * @author lx
 * @version 1.0   
 */
trait JobListener {

  /**
   * one resultTask to one this method
   * @param index 对应分区任务
   * @param result 任务结果
   */
  def taskSucceeded(index:Int,result:Any):Unit

  def jobFailed(exception:Exception):Unit

}
