package org.lxbigdata.sparklet.rdd

import org.lxbigdata.sparklet.{Partition, SparkletContext, TaskContext}

import java.nio.file.{DirectoryStream, Files, Path}
import scala.io.{BufferedSource, Source}
import scala.reflect.ClassTag

/**
 * ClassName: FileRDD
 * Package: org.lxbigdata.sparklet.rdd
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class FileRDD [T:ClassTag](sc:SparkletContext,path:String) extends RDD[T](sc,List()){

  override def getPartitions: Array[Partition] = {
    import scala.collection.JavaConverters._
    val directoryStream = Files.newDirectoryStream(Path.of(path))
    var i = 0
    directoryStream
      .iterator()
      .asScala.
      filter(_.toFile.length > 0)
      .map(file => {
        //localFile:目录下的一个小文件对应一个partition
        //这里可以使用自增id
        val partition = new FilePartition(id, i, file.toFile.getAbsolutePath)
        i += 1
        partition
      }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val partition = getPartitions(split.index).asInstanceOf[FilePartition]
    val file: BufferedSource = Source.fromFile(partition.location)
    //这里由于localFileRDD的参数有classTag为string,所以运行的时候不擦除。这里数据采取lazy读取
    file.getLines().asInstanceOf[Iterator[T]]
  }
}

class FilePartition(rddId:Int,override val index:Int,var location:String) extends Partition {
  //保证每个分区的hashcode值唯一
  override def hashCode(): Int = 31*(31*rddId) + index
}