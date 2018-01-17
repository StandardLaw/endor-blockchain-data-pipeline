package com.endor.storage.io
import java.io.{File, _}
import java.nio.file.{Files, StandardCopyOption}

import org.apache.commons.io.FileUtils

import scala.reflect.io.Path
import scala.util.{Random, Try}

/**
  * Created by user on 5/22/16.
  */
class LocalIOHandler(val baseDir: String = {
    val dir = List(System.getProperty("java.io.tmpdir"), Random.alphanumeric.take(20).mkString)
      .mkString(File.separator)

    Path(dir).createDirectory(failIfExists = true).toAbsolute.toString()
  }) extends IOHandler {

  Path(baseDir).ensuring(_.exists)

  override def ensureCorrectURIScheme(fullURI: String): String = {
    if (fullURI.startsWith("s3")) {
      super.ensureCorrectURIScheme(fullURI)
    } else {
      fullURI
    }
  }

  override def getReadPathFor(path: String): String = baseDir + File.separator + path

  override def pathExists(path: String): Boolean = Path(Seq(baseDir, path).mkString(File.separator)).exists

  override def loadRawData(path: String): InputStream = new FileInputStream(Path(Seq(baseDir, path).mkString(File.separator)).toString)

  override def saveRawData(file: File, path: String): Unit = {
    val fullPath = Path(Seq(baseDir, path).mkString(File.separator)).toString
    new File(new File(fullPath).getParent).mkdirs
    val writer = new FileWriter(new File(fullPath))
    writer.write(scala.io.Source.fromInputStream(new FileInputStream(file)).mkString)
    writer.close()
  }

  override def saveRawData(input: InputStream, path: String): Unit = {
    val fullPath = Path(Seq(baseDir, path).mkString(File.separator)).toString
    new File(new File(fullPath).getParent).mkdirs
    val writer = new FileWriter(new File(fullPath))
    writer.write(scala.io.Source.fromInputStream(input).mkString)
    writer.close()
  }

  override def listFiles(path: String): List[FileInfo] = {
    val currentFile = if(path.startsWith(baseDir)){
      new File(path)
    } else {
      new File(getReadPathFor(path))
    }

    if (currentFile.exists && currentFile.isDirectory) {
      currentFile.listFiles.flatMap{
        case f if f.isFile => FileInfo(f.getAbsolutePath.replace(baseDir, ""), f.length()) :: Nil
        case d if d.isDirectory => listFiles(d.getAbsolutePath)
      } toList
    } else {
      FileInfo(currentFile.getAbsolutePath.replace(baseDir, ""), currentFile.length()) :: Nil
    }
  }

  override def extractPathFromFullURI(fullURI: String): String = {
    val pathRegex = "(.*)/[*]".r
    fullURI.replace(baseDir, "") match {
      case pathRegex(path) => path
      case path => path
    }
  }

  override def deleteFilesByPredicate(prefix: String, predicate: String => Boolean): Unit = {
    listFiles(prefix).map(x => (x.key, x)).filter(x => predicate(x._1)).foreach {
      case (_, fileInfo) => FileUtils.deleteQuietly(new File(getReadPathFor(fileInfo.key)))
    }

  }

  override def moveFile(sourceFilePath: String, destFilePath: String): Unit = {
    Files.move(
      Path(getReadPathFor(sourceFilePath)).jfile.toPath,
      Path(getWritePathFor(destFilePath)).jfile.toPath,
      StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING
    )
  }

  def introduceDataFromLocalFolder(localPath: String): Unit = {
    val rootPath = new File(localPath)

    if (rootPath.exists()) {
      if (rootPath.isDirectory){
        rootPath.listFiles().foreach(file => Try {saveRawData(file, file.getAbsolutePath)})
      } else {
        saveRawData(rootPath, rootPath.getAbsolutePath)
      }
    }
  }
}

trait LocalIoComponent {
  private lazy val ioHandler = new LocalIOHandler()

  protected lazy val ioHandlers: Map[String, IOHandler] = Map[String, IOHandler]().withDefaultValue(ioHandler)
}