package saprklet.utils

import org.junit.jupiter.api.Test

import java.nio.file.{Files, Path}
import scala.collection.JavaConverters._
/**
 * ClassName: FilesTest01
 * Package: saprklet.utils
 * Description: 
 *
 * @author lx
 * @version 1.0   
 */
class FilesTest01 {
  @Test
  def test01(): Unit = {
    val directoryStream = Files.newDirectoryStream(Path.of("data"))
    directoryStream.iterator().asScala.map(p => p.toAbsolutePath.toString).foreach(println)
  }

}
