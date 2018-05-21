package com.endor.storage.dataset

import java.io.File

import com.endor._
import com.endor.infra.spark.SparkDriverFunSuite
import com.endor.storage.io.LocalIOHandler
import com.endor.storage.sources._
import org.apache.spark.sql.{Encoder, Encoders, functions => F}
import org.scalatest.FunSuite

final case class Person(name: String, age: Int)

object Person {
  implicit val encoder: Encoder[Person] = Encoders.product[Person]
}

class FileSystemDatasetStoreTest extends FunSuite with SparkDriverFunSuite {
  test("Loading a dataset without internal should return exactly the same columns as the dataset's type") {
    val data = (0 to 10).map(_ => Person(randomString(10), randomGenerator.nextInt()))
    val ds = spark.createDataset(data)
    val store = {
      val tempDir = createTempDir("data1")
      new File(tempDir).mkdir()
      val ioHandler = new LocalIOHandler(tempDir)
      new FileSystemDatasetStore(ioHandler, spark)
    }
    val dataKey: DataKey[Person] = DataKey(CustomerId("customer"), DataId[Person]("data"))
    store.storeParquet(dataKey.onBoarded, ds.withColumn("randCol", F.rand()).as[Person])
    val loaded = store.loadParquet(dataKey.onBoarded)
    loaded.toDF().columns should contain theSameElementsAs ds.columns
  }
}
