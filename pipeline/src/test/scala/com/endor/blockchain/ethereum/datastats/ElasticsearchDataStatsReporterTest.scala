package com.endor.blockchain.ethereum.datastats

import java.sql.{Date, Timestamp}

import com.endor.blockchain.ethereum.tokens.AggregatedRates
import com.endor.blockchain.ethereum.transaction.ProcessedTransaction
import com.endor.infra.spark.SparkDriverFunSuite
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.storage.dataset.BatchLoadOption
import com.endor.storage.sources._
import com.endor.{CustomerId, DataId, DataKey}
import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{HttpClient, Response}
import org.apache.spark.sql.{Dataset, SparkSession, functions => F}
import play.api.libs.json.{Json, Reads}

trait ElasticsearchDataStatsReporterTestComponent extends ElasticsearchDataStatsReporterComponent with BaseComponent {
  override val diConfiguration: DIConfiguration = DIConfiguration.ALL_IN_MEM
}

class ElasticsearchDataStatsReporterTest extends SparkDriverFunSuite {

  case class Node(client: HttpClient, ip: String, port: String)

  private def clientResponseToObjects[T: Reads](response: Response[SearchResponse]): Seq[T] = {
    response.result.hits.hits.map(_.sourceAsString).map(Json.parse).map(_.as[T])
  }

  private def createNode(): Node = {
    val localNode = LocalNode("testcluster", createTempDir(randomString(10)))
    val client: HttpClient = localNode.http(true)
    val Array(ip, port) = localNode.ipAndPort.split(":")
    Node(client, ip, port)
  }

  private def createContainer(): ElasticsearchDataStatsReporterTestComponent =
    new ElasticsearchDataStatsReporterTestComponent {
      override implicit def spark: SparkSession = ElasticsearchDataStatsReporterTest.this.spark
    }

  test("Test tansactions blocks 5609255-5609260") {
    val sess = spark
    import sess.implicits._
    val container = createContainer()
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val Node(client, ip, port) = createNode()
    val batchId = "my_batch"
    val dataKeyTransaction = DataKey[ProcessedTransaction](CustomerId("testCustomer"), DataId("testDsTransaction"))
    val dataKeyRates = DataKey[AggregatedRates](CustomerId("testCustomer"), DataId("testDsRates"))
    val config = ElasticsearchDataStatsConfig(DatasetDefinition(dataKeyTransaction, BatchLoadOption.UseExactly(Seq(batchId))),
      DatasetDefinition(dataKeyRates, BatchLoadOption.UseExactly(Seq(batchId))), "test1", ip, port.toInt)

    val inputPath = this.getClass.getResource("/com/endor/blockchain/ethereum/blocks/parsed/5609255-5609260.parquet").toString
    val inputDs = spark.read.parquet(inputPath).as[ProcessedTransaction]
    container.datasetStore.storeParquet(dataKeyTransaction.onBoarded,
      inputDs.withColumn("batch_id", F.lit(batchId)).as[ProcessedTransaction])
    container.datasetStore.storeParquet(dataKeyRates.onBoarded,
      spark.emptyDataset[AggregatedRates].withColumn("batch_id", F.lit(batchId)).as[AggregatedRates])


    container.driver.run(config = config)
    val ts = new Timestamp(0).toInstant.toString
    val selfComputed = inputDs
      .groupByKey(_.blockNumber)
      .mapGroups {
        (blockNumber, txIt) =>
          val a = txIt.foldLeft(BlockStatsV1(blockNumber, ts, 0, Seq.empty)) {
            case (acc, tx) => acc.copy(numTx = acc.numTx + 1, date = tx.timestamp.toInstant.toString, addresses = acc.addresses :+ tx.sendAddress)
          }
          a.copy(numTx = a.numTx / 2, addresses = a.addresses.distinct)
      }

    val expected = selfComputed.collect()
    val resp = client.execute {
      searchWithType(config.elasticsearchIndex / implicitly[EsType[BlockStatsV1]].esType) limit expected.length + 1
    }.await
    resp.isRight should be(true)
    resp match {
      case Right(success) =>
        val results = clientResponseToObjects[BlockStatsV1](success)
        results should contain theSameElementsAs expected
      case _ =>
    }
    client.close()
  }

  private def runAndCompare(inputDs: Dataset[AggregatedRates], container: ElasticsearchDataStatsReporterTestComponent,
                            config: ElasticsearchDataStatsConfig, dataKeyRates: DataKey[AggregatedRates],
                            dateStr: String, client: HttpClient, batchId: String): Unit = {
    val sess = spark
    import sess.implicits._
    val date = Date.valueOf(dateStr)
    val inputDSFirst = inputDs.filter($"date" < date)
    container.datasetStore.storeParquet(dataKeyRates.onBoarded,
      inputDSFirst.withColumn("batch_id", F.lit(batchId)).as[AggregatedRates])
    container.driver.run(config = config)


    val respFirst = client.execute {
      searchWithType(config.elasticsearchIndex / implicitly[EsType[AggregatedRates]].esType) limit inputDSFirst.count().toInt + 100
    }.await
    respFirst.isRight should be(true)
    respFirst match {
      case Right(success) =>
        val results1 = clientResponseToObjects[AggregatedRates](success)
        results1 should contain theSameElementsAs inputDSFirst.collect()
      case _ =>
    }
  }
  test("Basic: Test rates single day") {
    val sess = spark
    import sess.implicits._
    val container = createContainer()
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val Node(client, ip, port) = createNode()
    val batchId = "my_batch_basic"
    val dataKeyTransaction = DataKey[ProcessedTransaction](CustomerId("testCustomerBasic: Test rates single Day"), DataId("testDsTransactionBasic: Test rates single Day"))
    val dataKeyRates = DataKey[AggregatedRates](CustomerId("testCustomer1"), DataId("testDsRates1"))
    val inputPath = this.getClass.getResource("/com/endor/blockchain/ethereum/rates/single_day.parquet").toString
    val inputDs = spark.read.parquet(inputPath)
      .withColumn("date", F.to_date($"date", "yyyy-MM-dd"))
      .as[AggregatedRates]

    val configFirst = ElasticsearchDataStatsConfig(DatasetDefinition(dataKeyTransaction, BatchLoadOption.UseExactly(Seq(batchId))),
      DatasetDefinition(dataKeyRates, BatchLoadOption.UseExactly(Seq(batchId))), "test1", ip, port.toInt)
    container.datasetStore.storeParquet(dataKeyTransaction.onBoarded,
      spark.emptyDataset[ProcessedTransaction].withColumn("batch_id", F.lit(batchId)).as[ProcessedTransaction])
    runAndCompare(inputDs, container, configFirst, dataKeyRates, "2019-01-01", client, batchId)
    client.close()

  }

  test("Advanced: Test rates with Maxdate") {
    // init
    val sess = spark
    import sess.implicits._
    val container = createContainer()
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val Node(client, ip, port) = createNode()
    val batchId = "my_batch"
    val dataKeyTransaction = DataKey[ProcessedTransaction](CustomerId("testCustomerAdvanced: Test rates with Maxdate"),
      DataId("testDsTransactionAdvanced: Test rates with Maxdate"))
    val dataKeyRates = DataKey[AggregatedRates](CustomerId("testCustomer"), DataId("testDsRates"))
    val inputPath = this.getClass.getResource("/com/endor/blockchain/ethereum/rates/2018-05-15-rates.parquet").toString
    val inputDs = spark.read.parquet(inputPath)
      .withColumn("date", F.to_date($"date", "yyyy-MM-dd"))
      .as[AggregatedRates]


    val configFirst = ElasticsearchDataStatsConfig(DatasetDefinition(dataKeyTransaction, BatchLoadOption.UseExactly(Seq(batchId))),
      DatasetDefinition(dataKeyRates, BatchLoadOption.UseExactly(Seq(batchId))), "test1", ip, port.toInt)
    container.datasetStore.storeParquet(dataKeyTransaction.onBoarded,
      spark.emptyDataset[ProcessedTransaction].withColumn("batch_id", F.lit(batchId)).as[ProcessedTransaction])
    runAndCompare(inputDs, container, configFirst, dataKeyRates, "2016-01-01", client, batchId)


    val configSecond = ElasticsearchDataStatsConfig(DatasetDefinition(dataKeyTransaction, BatchLoadOption.UseExactly(Seq(batchId))),
      DatasetDefinition(dataKeyRates, BatchLoadOption.UseExactly(Seq(batchId))), "test2", ip, port.toInt)
    runAndCompare(inputDs, container, configSecond, dataKeyRates, "2017-01-01", client, batchId)
    client.close()
  }
}
