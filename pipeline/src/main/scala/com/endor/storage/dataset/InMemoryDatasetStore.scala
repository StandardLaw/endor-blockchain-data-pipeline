package com.endor.storage.dataset

import com.endor._
import com.endor.storage.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import scala.collection.mutable
import scala.util.matching.Regex

class InMemoryDatasetStore extends DatasetStore {
  private val internalMap = mutable.Map[String, Dataset[_]]()

  private def store[T : Encoder](path: String, dataFrame: Dataset[T], saveMode: SaveMode): Unit = {
    val cached = dataFrame.cache()
    // force run at each store.
    cached.collect()

    val finalDf = saveMode match {
      case SaveMode.Append if internalMap.contains(path) => cached.union(internalMap(path).as[T])
      case SaveMode.Ignore if internalMap.contains(path) => internalMap(path)
      case SaveMode.ErrorIfExists if internalMap.contains(path) => throw new RuntimeException()
      case _ => cached
    }

    internalMap += path -> finalDf
  }

  private def load(path: String): DataFrame = {
    val pathRegex = new Regex("^" + path.replace("*", ".*") + "$")

    internalMap
      .collect { case (key, value) if (pathRegex findFirstIn key).isDefined || key === path => value }
      .map(x => x.as[Row](RowEncoder(x.schema)))
      .reduceOption(_ union _)
      .getOrElse {
        // Mimic Spark's behavior here
        throw new AnalysisException(s"Path does not exist: $path") {}
      }
  }

  override def storeParquet[T : Encoder](parquet: DatasetSource[T] with ParquetSourceType,
                                         dataFrame: Dataset[T],
                                         partitionBy: Seq[String] = Seq.empty,
                                         saveMode: SaveMode = SaveMode.Overwrite): Unit =
    store(parquet.path, dataFrame, saveMode)

  override def doLoadParquet(parquet: DatasetSource[_] with ParquetSourceType): DataFrame =
    load(parquet.path)

  override def datasetExists(source: DatasetSource[_]): Boolean = {
    val path = source.path
    val pathRegex = new Regex("^" + path.replace("*", ".*") + "$")

    internalMap
      .exists(x => (pathRegex findFirstIn x._1).isDefined)
  }
}
