package com.endor.storage.io
import java.io.{ByteArrayInputStream, File, FileNotFoundException, InputStream}
import java.nio.file.{Files, Paths}

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
  * Created by user on 5/19/16.
  */
class InMemoryIOHandler extends IOHandler {
  val internalMap: mutable.Map[String, Array[Byte]] = mutable.Map()

  override def getReadPathFor(path: String): String = path

  override def loadRawData(path: String): InputStream = {
    new ByteArrayInputStream(internalMap(path))
  }

  override def saveRawData(file: File, path: String): Unit = {
    internalMap += path -> Files.readAllBytes(Paths.get(path))

  }

  override def saveRawData(input: InputStream, path: String): Unit = {
    internalMap += path -> Stream.continually(input.read).takeWhile(_ != -1).map(_.toByte).toArray
  }

  override def pathExists(path: String): Boolean = {
    internalMap.contains(path)
  }

  override def listFiles(path: String): List[FileInfo] = {
    internalMap.filter(_._1.startsWith(path)).map {
      case (filePath, data) => FileInfo(filePath, data.length.toLong)
    } toList
  }

  def introduceDataFromLocalFolder(localPath: String): Unit = {
    val rootPath = new File(localPath)

    if (rootPath.exists()) {
      rootPath.listFiles().foreach(file => saveRawData(file, file.getAbsolutePath))
    }
  }

  override def extractPathFromFullURI(fullURI: String): String = fullURI

  override def deleteFilesByPredicate(prefix: String, predicate: String => Boolean): Unit = {
    internalMap.keys.filter(predicate).foreach(internalMap.remove)
  }

  override def moveFile(sourceFilePath: String, destFilePath: String): Unit = {
    internalMap.remove(sourceFilePath) match {
      case None =>
        Failure(new FileNotFoundException(sourceFilePath))
      case Some(contents) =>
        internalMap.put(destFilePath, contents)
        Success(())
    }
  }
}

trait InMemoryIoComponent {
  private lazy val ioHandler = new InMemoryIOHandler()

  protected lazy val ioHandlers: Map[String, IOHandler] = Map[String, IOHandler]().withDefaultValue(ioHandler)
}

