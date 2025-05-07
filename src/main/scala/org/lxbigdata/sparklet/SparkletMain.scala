package org.lxbigdata.sparklet

import org.lxbigdata.sparklet.rdd.RDD

import java.util.logging.{ConsoleHandler, FileHandler, Logger}
/**
 * ClassName: SparkletMain
 * Package: org.lxbigdata.sparklet
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
object SparkletMain {
  val logger = Logger.getLogger(this.getClass.getName)
  logger.addHandler(new ConsoleHandler())
  logger.setUseParentHandlers(false)
  logger.setLevel(java.util.logging.Level.INFO)

  def main(args: Array[String]): Unit = {
    val sparkletConf = new SparkletConf().setMaster("local[*]")
      .setTmpDir("tmp\\")
      .setAppName("sparklet")
    val sc = new SparkletContext(sparkletConf)
    val lines = sc.textFile("data")
    val value: RDD[(String, Int)] = lines.flatMap(line => line.split(" "))
      .filter(word => word.length > 0)
      .map(word => (word, 1))
    val result = value.reduceByKey(_ + _)
    val tuples: Array[(String, Int)] = result.collect()
    logger.info(s"the length is ${tuples.length}")
    tuples.foreach(tuple => logger.info(s"${tuple._1} ${tuple._2}"))
  }
}
