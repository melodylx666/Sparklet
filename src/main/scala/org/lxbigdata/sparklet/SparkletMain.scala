package org.lxbigdata.sparklet

/**
 * ClassName: SparkletMain
 * Package: org.lxbigdata.sparklet
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
object SparkletMain extends App {
  val sparkletConf = new SparkletConf()
    .setMaster("local")
    .setAppName("SparkletMain")

  new SparkletContext(sparkletConf)

}
