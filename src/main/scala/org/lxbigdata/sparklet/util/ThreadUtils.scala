package org.lxbigdata.sparklet.util

import scala.concurrent.{Awaitable, TimeoutException}
import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
 * ClassName: ThreadUtils
 * Package: org.lxbigdata.sparklet.util
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
object ThreadUtils {
  def awaitReady[T](awaitable: Awaitable[T], atMost: Duration): awaitable.type = {
    try {
      val awaitPermission = null.asInstanceOf[scala.concurrent.CanAwait]
      awaitable.ready(atMost)(awaitPermission)
    } catch {
      // TimeoutException is thrown in the current thread, so not need to warp the exception.
      case NonFatal(t) if !t.isInstanceOf[TimeoutException] =>
        throw new Exception(s"Exception in awaitResult:${t.getMessage}")
    }
  }
}
