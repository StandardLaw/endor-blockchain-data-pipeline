package com.endor.storage.dataset

import com.endor.storage.sources._
import com.endor.serialization._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{functions => F}
import play.api.libs.json.{Json, OFormat}

sealed trait BatchLoadOption

object BatchLoadOption {
  case object UseAll extends BatchLoadOption
  final case class UseExactly(batchIds: Seq[String]) extends BatchLoadOption
  final case class UseExcept(batchIds: Seq[String]) extends BatchLoadOption

  implicit val format: OFormat[BatchLoadOption] = formatFor(
    "UseAll" -> UseAll,
    "UseExactly" -> Json.format[UseExactly],
    "UseExcept" -> Json.format[UseExcept]
  )
}

trait DatasetStore {
  protected def doLoadParquet(parquet: DatasetSource[_] with ParquetSourceType): DataFrame
  def storeParquet[T : Encoder](parquet: DatasetSource[T] with ParquetSourceType, dataset: Dataset[T],
                                partitionBy: Seq[String] = Seq.empty, saveMode: SaveMode = SaveMode.Overwrite): Unit
  def datasetExists(source: DatasetSource[_]): Boolean

  // Parquet Templates
  final def loadParquet[T](parquet: DatasetSource[T] with ParquetSourceType,
                           withInternal: Boolean = false,
                           batchLoadOption: BatchLoadOption = BatchLoadOption.UseAll)
                          (implicit encoder: Encoder[T]): Dataset[T] = {
    val rawDf = doLoadParquet(parquet)
    val batchFilteredDf = batchLoadOption match {
      case BatchLoadOption.UseAll => rawDf
      case BatchLoadOption.UseExactly(batchIds) => rawDf.where(F.col("batch_id") isin(batchIds: _*))
      case BatchLoadOption.UseExcept(batchIds) => rawDf.where(!(F.col("batch_id") isin(batchIds: _*)))
    }

    val convertedTimestampsToDatesDf = {
      val timestampColumns = batchFilteredDf.schema.filter(_.dataType == DataTypes.TimestampType).map(_.name).toSet
      val dateColumnsInEncoder = encoder.schema.filter(_.dataType == DataTypes.DateType).map(_.name).toSet
      val columnsToConvert = timestampColumns intersect dateColumnsInEncoder
      columnsToConvert.foldLeft(batchFilteredDf) {
        case (df, col) => df.withColumn(col, F.to_date(F.col(col)))
      }
    }

    if (withInternal) {
      convertedTimestampsToDatesDf.as[T]
    } else {
      convertedTimestampsToDatesDf.select(encoder.schema.map(_.name).map(F.col): _*).as[T]
    }
  }

  final def loadUntypedParquet(parquet: DataFrameSource with ParquetSourceType): DataFrame =
    doLoadParquet(parquet)

  // Parquet Templates
  final def storeUntypedParquet(parquet: DataFrameSource with ParquetSourceType, dataFrame: DataFrame,
                                partitionBy: Seq[String] = Seq.empty, saveMode: SaveMode = SaveMode.Overwrite): Unit =
    storeParquet[Row](parquet, dataFrame, partitionBy, saveMode)(RowEncoder(dataFrame.schema))

  def delete(source: DatasetSource[_]): Unit = {}
}