package com.endor.storage.sources

sealed trait TextFileSource extends HasPath

object TextFileSource {
  def apply(hasPath: HasPath): TextFileSource = new TextFileSource {
    override val path: String = hasPath.path
  }
}

final case class JsonFileSource[T](path: String) extends TextFileSource

object JsonFileSource {
  def apply[T](hasPath: HasPath): JsonFileSource[T] = JsonFileSource[T](hasPath.path)
}

final case class CsvFileSource(path: String) extends TextFileSource

object CsvFileSource {
  def apply(hasPath: HasPath): CsvFileSource = CsvFileSource(hasPath.path)
}

final class FileSource private[sources](val path: String) extends HasSubPaths {
  def contains[T <: DataFrameSourceType](dataFrameType: DatasetType[T]): DatasetSourceBuilder[T] =
    dataFrameType.from(this)
}

