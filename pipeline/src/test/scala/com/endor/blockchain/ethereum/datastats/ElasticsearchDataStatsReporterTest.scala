package com.endor.blockchain.ethereum.datastats

import java.sql.{Date, Timestamp}
import java.time.Instant

import com.endor.blockchain.ethereum.blocksummaries.Transaction
import com.endor.blockchain.ethereum.tokens.AggregatedRates
import com.endor.infra.spark.SparkDriverSuite
import com.endor.infra.{BaseComponent, DIConfiguration}
import com.endor.storage.dataset.BatchLoadOption
import com.endor.storage.sources._
import com.endor.{CustomerId, DataId, DataKey}
import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{HttpClient, Response}
import org.apache.spark.sql.{Dataset, SparkSession, functions => F}
import org.scalatest.{EitherValues, Outcome, fixture}
import play.api.libs.json.{Json, Reads}

trait ElasticsearchDataStatsReporterTestComponent extends ElasticsearchDataStatsReporterComponent with BaseComponent {
  override val diConfiguration: DIConfiguration = DIConfiguration.ALL_IN_MEM
}

class ElasticsearchDataStatsReporterTest extends fixture.FunSuite with SparkDriverSuite with EitherValues {
  case class Node(client: HttpClient, ip: String, port: String)

  override def beforeAll(): Unit = {
    super.beforeAll()
    System.setProperty("es.set.netty.runtime.available.processors", "false")
  }

  override protected def withFixture(test: OneArgTest): Outcome = {
    val node = createNode()
    try {
      test(node)
    }
    finally node.client.close()
  }
  override type FixtureParam = Node
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

  test("Test tansactions two blocks 46147,46169") { node =>
    val sess = spark
    import sess.implicits._
    val container = createContainer()
    val batchId = "my_batch"
    val nowTs = new Date(Instant.now().toEpochMilli).toString
    val dataKeyTransaction = DataKey[Transaction](CustomerId("testCustomer"), DataId("testDsTransaction"))
    val dataKeyRates = DataKey[AggregatedRates](CustomerId("testCustomer"), DataId("testDsRates"))
    val config = ElasticsearchDataStatsConfig(
      DatasetDefinition(dataKeyTransaction, BatchLoadOption.UseExactly(Seq(batchId))),
      DatasetDefinition(dataKeyRates, BatchLoadOption.UseExactly(Seq(batchId))),
      "test1", node.ip, node.port.toInt, nowTs)

    val inputPath = this.getClass.getResource("/com/endor/blockchain/ethereum/blocks/parsed/two_blocks.parquet").toString
    val inputDs = spark.read.parquet(inputPath).as[Transaction]
    container.datasetStore.storeParquet(dataKeyTransaction.onBoarded,
      inputDs.withColumn("batch_id", F.lit(batchId)).as[Transaction])
    container.datasetStore.storeParquet(dataKeyRates.onBoarded,
      spark.emptyDataset[AggregatedRates].withColumn("batch_id", F.lit(batchId)).as[AggregatedRates])


    container.driver.run(config = config)
    val ts = new Timestamp(0).toInstant.toString
    val selfComputed = inputDs
      .groupByKey(_.blockNumber)
      .mapGroups {
        (blockNumber, txIt) =>
          val a = txIt.foldLeft(EthereumBlockStatsV1(blockNumber, ts, nowTs,0, Seq.empty)) {
            case (acc, tx) => acc.copy(numTx = acc.numTx + 1, date = tx.timestamp.toInstant.toString, addresses = acc.addresses :+ tx.sendAddress)
          }
          a.copy(numTx = a.numTx / 2, addresses = a.addresses.distinct)
      }

    val expected = selfComputed.collect()
    val resp = node.client.execute {
      val esType = implicitly[EsType[EthereumBlockStatsV1]]
      searchWithType(esType.indexName / esType.typeVersion) limit expected.length + 1
    }.await
    clientResponseToObjects[EthereumBlockStatsV1](resp.right.value) should contain theSameElementsAs expected

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

    val response = client.execute {
      val esType = implicitly[EsType[ERC20RatesStatsV1]]
      searchWithType(esType.indexName / esType.typeVersion) limit inputDSFirst.count().toInt + 100
    }.await
    response.isRight should be(true)
    response match {
      case Right(success) =>
        val results = clientResponseToObjects[ERC20RatesStatsV1](success).distinct
        val expected = inputDSFirst
          .withColumn("publishedOn", F.lit(config.publishedOn))
          .as[ERC20RatesStatsV1]
          .collect()
        val intersect = results.filterNot(expected.contains)
        val _ = intersect
        results should contain theSameElementsAs expected
      case _ =>
    }
  }
  test("Basic: Test rates single day") { node =>
    val sess = spark
    import sess.implicits._
    val container = createContainer()
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val batchId = "my_batch_basic"
    val nowTs = new Date(Instant.now().toEpochMilli).toString
    val dataKeyTransaction = DataKey[Transaction](CustomerId("testCustomerBasic: Test rates single Day"), DataId("testDsTransactionBasic: Test rates single Day"))
    val dataKeyRates = DataKey[AggregatedRates](CustomerId("testCustomer1"), DataId("testDsRates1"))
    val inputPath = this.getClass.getResource("/com/endor/blockchain/ethereum/rates/single.parquet").toString
    val inputDs = spark.read.parquet(inputPath)
      .withColumn("date", F.to_date($"date", "yyyy-MM-dd"))
      .as[AggregatedRates]

    val configFirst = ElasticsearchDataStatsConfig(
      DatasetDefinition(dataKeyTransaction, BatchLoadOption.UseExactly(Seq(batchId))),
      DatasetDefinition(dataKeyRates, BatchLoadOption.UseExactly(Seq(batchId))),
      "test1", node.ip, node.port.toInt, nowTs)
    container.datasetStore.storeParquet(dataKeyTransaction.onBoarded,
      spark.emptyDataset[Transaction].withColumn("batch_id", F.lit(batchId)).as[Transaction])
    runAndCompare(inputDs, container, configFirst, dataKeyRates, "2019-01-01", node.client, batchId)
  }

  test("Advanced: Test rates with Maxdate") { node =>
    // init
    val sess = spark
    import sess.implicits._
    val container = createContainer()
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val batchId = "my_batch"
    val dataKeyTransaction = DataKey[Transaction](CustomerId("testCustomerAdvanced: Test rates with Maxdate"),
      DataId("testDsTransactionAdvanced: Test rates with Maxdate"))
    val dataKeyRates = DataKey[AggregatedRates](CustomerId("testCustomer"), DataId("testDsRates"))
    container.datasetStore.storeParquet(dataKeyTransaction.onBoarded,
      spark.emptyDataset[Transaction].withColumn("batch_id", F.lit(batchId)).as[Transaction])
    val inputPath = this.getClass.getResource("/com/endor/blockchain/ethereum/rates/2018-05-15-rates.parquet").toString
    val nowTs = new Date(Instant.now().toEpochMilli).toString
    val inputDs = spark.read.parquet(inputPath)
      .withColumn("date", F.to_date($"date", "yyyy-MM-dd"))
      .as[AggregatedRates]
    val configFirst = ElasticsearchDataStatsConfig(
      DatasetDefinition(dataKeyTransaction, BatchLoadOption.UseExactly(Seq(batchId))),
      DatasetDefinition(dataKeyRates, BatchLoadOption.UseExactly(Seq(batchId))),
      "test1", node.ip, node.port.toInt, nowTs)
    runAndCompare(inputDs, container, configFirst, dataKeyRates, "2016-01-01", node.client, batchId)


    val configSecond = configFirst.copy(elasticsearchIndex = "test2")
    runAndCompare(inputDs, container, configSecond, dataKeyRates, "2016-01-02", node.client, batchId)

    val configThree = configFirst.copy(elasticsearchIndex = "test3")
    runAndCompare(inputDs, container, configThree, dataKeyRates, "2016-02-01", node.client, batchId)
  }
}
