package org.lxbigdata.sparklet

import org.lxbigdata.sparklet.rdd.RDD

/**
 * ClassName: SparkletMain
 * Package: org.lxbigdata.sparklet
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
object SparkletMain {
  def main(args: Array[String]): Unit = {
    val sparkletConf = new SparkletConf().setMaster("local[*]")
      .setTmpDir("tmp\\")
      .setAppName("sparklet")
    val sc = new SparkletContext(sparkletConf)
    val lines = sc.textFile("data/test.txt")
    val value: RDD[(String, Int)] = lines.flatMap(line => line.split(" "))
      .filter(word => word.length > 0)
      .map(word => (word, 1))
    val result = value.reduceByKey(_ + _)
    val tuples: Array[(String, Int)] = result.collect()
    tuples.foreach(tuple => println(tuple._1 + ":" + tuple._2))
  }
}
