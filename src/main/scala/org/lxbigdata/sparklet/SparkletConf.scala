package org.lxbigdata.sparklet

import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentHashMap

/**
 * ClassName: SparkletConf
 * Package: org.lxbigdata.sparklet
 * Description:
 *
 * @author lx
 * @version 1.0
 */
class SparkletConf() {
  private val settings = new ConcurrentHashMap[String, String]()

  def set(key: String, value: String): SparkletConf = {
    settings.put(key, value)
    this
  }

  def setAppName(name: String): SparkletConf = {
    set("sparklet.app.name", name)
  }
  def setMaster(master: String): SparkletConf = {
    set("sparklet.master", master)
  }

  def setTmpDir(tmpDir: String): SparkletConf = {
    try{
      val path: Path = Files.createDirectory(Path.of(tmpDir))
    }catch {
      case e: Exception =>
        throw new Exception("tmpDir is not a valid directory")
    }
    set("sparklet.tmp.dir", tmpDir)
  }

  def get(key: String):Option[String] = {
    Option(settings.get(key)) //可能为空，所以用Option包装一下
  }
}
