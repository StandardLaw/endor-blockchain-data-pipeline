package com.endor.storage.dataset

import com.endor.storage.io.IOHandler
import com.endor.storage.sources._
import org.apache.spark.sql._


class FileSystemDatasetStore(ioHandler: IOHandler, spark: SparkSession) extends DatasetStore {
  override def doLoadParquet(parquet: DatasetSource[_] with ParquetSourceType): DataFrame =
    spark.read.parquet(ioHandler.getReadPathFor(parquet.path))

  override def storeParquet[T : Encoder](parquet: DatasetSource[T] with ParquetSourceType,
                                         dataset: Dataset[T],
                                         partitionBy: Seq[String] = Seq.empty,
                                         saveMode: SaveMode = SaveMode.Overwrite): Unit = {

    val realPath = ioHandler.getWritePathFor(parquet.path)

    if (partitionBy.nonEmpty) {
      dataset.write.mode(saveMode).partitionBy(partitionBy: _*).parquet(realPath)
    } else {
      dataset.write.mode(saveMode).parquet(realPath)
    }
  }

  override def datasetExists(source: DatasetSource[_]): Boolean = {
    ioHandler.pathExists(source.path + "/_SUCCESS")
  }
}