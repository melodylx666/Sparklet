package org.lxbigdata.sparklet.util

import org.lxbigdata.sparklet.TaskContext

/**
 * ClassName: InterruptibleIterator
 * Package: org.lxbigdata.sparklet.util
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class InterruptibleIterator[+T](val context: TaskContext, val delegate: Iterator[T])
  extends Iterator[T] {

  def hasNext: Boolean = {
    delegate.hasNext
  }

  def next(): T = delegate.next()
}
