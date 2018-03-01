package com.endor.storage.dataset

import com.endor.storage.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{functions => F}

trait DatasetStore {
  protected def doLoadParquet(parquet: DatasetSource[_] with ParquetSourceType): DataFrame
  def storeParquet[T : Encoder](parquet: DatasetSource[T] with ParquetSourceType, dataset: Dataset[T],
                                partitionBy: Seq[String] = Seq.empty, saveMode: SaveMode = SaveMode.Overwrite): Unit
  def datasetExists(source: DatasetSource[_]): Boolean

  // Parquet Templates
  final def loadParquet[T](parquet: DatasetSource[T] with ParquetSourceType,
                           withInternal: Boolean = false)
                          (implicit encoder: Encoder[T]): Dataset[T] = {
    val rawDf = doLoadParquet(parquet)
    if (withInternal) {
      rawDf.as[T]
    } else {
      rawDf.select(encoder.schema.map(_.name).map(F.col): _*).as[T]
    }
  }

  final def loadUntypedParquet(parquet: DataFrameSource with ParquetSourceType): DataFrame =
    doLoadParquet(parquet)

  // Parquet Templates
  final def storeUntypedParquet(parquet: DataFrameSource with ParquetSourceType, dataFrame: DataFrame,
                                partitionBy: Seq[String] = Seq.empty, saveMode: SaveMode = SaveMode.Overwrite): Unit =
    storeParquet[Row](parquet, dataFrame, partitionBy, saveMode)(RowEncoder(dataFrame.schema))
}