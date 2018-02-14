package com.endor.storage.sources

import org.apache.spark.sql.{Encoder, Row}

trait DatasetSource[T] extends HasPath {
  val successFilePath: String = this.path + "/_SUCCESS"
}

sealed trait CSVSchemaSource
object CSVSchemaSource {
  case object None extends CSVSchemaSource
  case object JSON extends CSVSchemaSource
}

sealed trait DataFrameSourceType
sealed trait ParquetSourceType extends DataFrameSourceType { this: DatasetSource[_] => }
sealed trait CsvSourceType extends DataFrameSourceType {
  this: DatasetSource[_] =>
  def options: Map[String, String] = Map("delimiter" -> ",", "header" -> true.toString)
  def schemaSource: CSVSchemaSource
}

trait DatasetSourceBuilder[T <: DataFrameSourceType] {
  def of[S : Encoder](): DatasetSource[S] with T
  def ofRows: DataFrameSource with T
}

sealed trait DatasetType[T <: DataFrameSourceType] {
  def from(source: FileSource): DatasetSourceBuilder[T]
}

case object parquet extends DatasetType[ParquetSourceType] {
  private final case class Concrete[S](path: String) extends DatasetSource[S] with ParquetSourceType

  override def from(source: FileSource): DatasetSourceBuilder[ParquetSourceType] =
    new DatasetSourceBuilder[ParquetSourceType] {
      override def of[S : Encoder](): DatasetSource[S] with ParquetSourceType = Concrete[S](source.path)
      override def ofRows: DataFrameSource with ParquetSourceType = Concrete[Row](source.path)
    }
}

sealed class CSVDatasetType(csvOptions: Map[String, String], schemaSource: CSVSchemaSource)
  extends DatasetType[CsvSourceType] {

  private final case class Concrete[S](path: String) extends DatasetSource[S] with CsvSourceType {
    override lazy val options: Map[String, String] = super.options ++ csvOptions
    override lazy val schemaSource: CSVSchemaSource = CSVDatasetType.this.schemaSource
  }

  override def from(source: FileSource): DatasetSourceBuilder[CsvSourceType] =
    new DatasetSourceBuilder[CsvSourceType] {
      override def of[S: Encoder](): DatasetSource[S] with CsvSourceType = Concrete[S](source.path)
      override def ofRows: DataFrameSource with CsvSourceType = Concrete[Row](source.path)
    }

  def withOptions(options: (String, String)*): CSVDatasetType =
    new CSVDatasetType(options.toMap, schemaSource)

  def withSchemaFromJson(): CSVDatasetType =
    new CSVDatasetType(csvOptions, CSVSchemaSource.JSON)
}

case object csv extends CSVDatasetType(Map.empty, CSVSchemaSource.None)