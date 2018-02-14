package com.endor.storage.dataset

import com.endor.storage.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

trait DatasetStore {
  protected def doLoadParquet(parquet: DatasetSource[_] with ParquetSourceType): DataFrame
  def storeParquet[T : Encoder](parquet: DatasetSource[T] with ParquetSourceType, dataset: Dataset[T],
                                partitionBy: Seq[String] = Seq.empty, saveMode: SaveMode = SaveMode.Overwrite): Unit
  def datasetExists(source: DatasetSource[_]): Boolean

  // Parquet Templates
  final def loadParquet[T : Encoder](parquet: DatasetSource[T] with ParquetSourceType): Dataset[T] =
    doLoadParquet(parquet).as[T]
  final def loadUntypedParquet(parquet: DataFrameSource with ParquetSourceType): DataFrame =
    doLoadParquet(parquet)

  // Parquet Templates
  final def storeUntypedParquet(parquet: DataFrameSource with ParquetSourceType, dataFrame: DataFrame,
                                partitionBy: Seq[String] = Seq.empty, saveMode: SaveMode = SaveMode.Overwrite): Unit =
    storeParquet[Row](parquet, dataFrame, partitionBy, saveMode)(RowEncoder(dataFrame.schema))
}