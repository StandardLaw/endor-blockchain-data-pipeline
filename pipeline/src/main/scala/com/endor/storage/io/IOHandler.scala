package com.endor.storage.io

import java.io.{ByteArrayInputStream, File, InputStream}

/**
  * Created by user on 5/19/16.
  */
trait IOHandler {
  def ensureCorrectURIScheme(fullURI: String): String = getReadPathFor(extractPathFromFullURI(fullURI))
  def extractPathFromFullURI(fullURI: String): String
  def getReadPathFor(path: String): String
  def getWritePathFor(path: String): String = getReadPathFor(path)
  def saveRawData(file: File, path: String): Unit
  def saveRawData(input: InputStream, path: String): Unit
  def saveRawData(input: String, path: String) : Unit =
    saveRawData(new ByteArrayInputStream(input.toString.getBytes), path)
  def loadRawData(path: String): InputStream
  def deleteFilesByPredicate(prefix: String, predicate: String => Boolean): Unit
  def listFiles(path: String): List[FileInfo]
  def pathExists(path: String): Boolean
  def getDataSize(inputPath: String): Long = {
    val path = extractPathFromFullURI(inputPath)
    listFiles(path).map(_.size).sum
  }
  def moveFile(sourceFilePath: String, destFilePath: String): Unit
}