package com.endor.storage.dataset

import java.io.File
import java.sql.{Date, Timestamp}
import java.time.Instant

import com.endor._
import com.endor.infra.spark.SparkDriverSuite
import com.endor.storage.io.LocalIOHandler
import com.endor.storage.sources._
import org.apache.spark.sql.{Encoder, Encoders, functions => F}
import org.scalatest.FunSuite

final case class Person(name: String, age: Int)

object Person {
  implicit val encoder: Encoder[Person] = Encoders.product
}

final case class Transaction(amount: Int, date: Timestamp)

object Transaction {
  implicit val encoder: Encoder[Transaction] = Encoders.product
}

final case class TransactionWithDate(amount: Int, date: Date)

object TransactionWithDate {
  implicit val encoder: Encoder[TransactionWithDate] = Encoders.product
}

class FileSystemDatasetStoreTest extends FunSuite with SparkDriverSuite {
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

  test("Can load datasets with date columns when stored timestamps") {
    val data = (0 to 10)
      .map(_ =>
        Transaction(randomGenerator.nextInt(100), randomDate(Timestamp.valueOf("2016-05-01 00:00:00"), Timestamp.from(Instant.now())))
      )
    val ds = spark.createDataset(data)
    val store = {
      val tempDir = createTempDir("data1")
      new File(tempDir).mkdir()
      val ioHandler = new LocalIOHandler(tempDir)
      new FileSystemDatasetStore(ioHandler, spark)
    }
    val storeDateKey = DataKey(CustomerId("customer"), DataId[Transaction]("data"))
    store.storeParquet(storeDateKey.onBoarded, ds)
    val loadDateKey = DataKey(CustomerId("customer"), DataId[TransactionWithDate]("data"))
    val loaded = store.loadParquet(loadDateKey.onBoarded)
    loaded.collect() should contain theSameElementsAs ds.collect().map(tx => TransactionWithDate(tx.amount, tx.date.toDate))
  }
}
