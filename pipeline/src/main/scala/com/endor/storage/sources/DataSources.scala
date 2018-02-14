package com.endor.storage.sources

import com.endor.DataKey
import org.apache.spark.sql.Encoder


trait DataSources {
  implicit final class DataFileSource[T: Encoder](dataKey: DataKey[T]) {
    def onBoarded: DatasetSource[T] with ParquetSourceType =
      dataKey.customer.sandbox / "OnBoarding" / dataKey.id.id / "data-parquet" contains parquet of[T]()
    def inbox: DatasetSource[T] with ParquetSourceType =
      dataKey.customer.source / dataKey.id.id / "Inbox" contains parquet of[T]()
  }
}