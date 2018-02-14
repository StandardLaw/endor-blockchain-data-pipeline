package com.endor.blockchain.ethereum.transaction

import java.sql.Timestamp

import com.endor.DataKey
import com.endor.blockchain.ethereum.ByteArrayUtil
import com.endor.infra.SparkSessionComponent
import com.endor.spark.blockchain._
import com.endor.spark.blockchain.ethereum._
import com.endor.spark.blockchain.ethereum.block.SimpleEthereumBlock
import com.endor.storage.dataset.{DatasetStore, DatasetStoreComponent}
import com.endor.storage.io.{IOHandler, IOHandlerComponent}
import com.endor.storage.sources._
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import play.api.libs.json.{Json, OFormat}

final case class EthereumTransactionsPipelineConfig(input: String, output: DataKey[ProcessedTransaction])

object EthereumTransactionsPipelineConfig {
  implicit val format: OFormat[EthereumTransactionsPipelineConfig] = Json.format[EthereumTransactionsPipelineConfig]
}

class EthereumTransactionsPipeline()
                                  (implicit spark: SparkSession, ioHandler: IOHandler, datasetStore: DatasetStore) {

  def createTransactionsDS(rawEthereum: Dataset[SimpleEthereumBlock]): Dataset[ProcessedTransaction] = {
    rawEthereum
      .flatMap {
        block =>
          val blockTime = new Timestamp(block.ethereumBlockHeader.timestamp * 1000)
          val blockNumber = block.ethereumBlockHeader.number
          block.ethereumTransactions
            .map(_.toEnriched)
            .flatMap {
              transaction =>
                val original = ProcessedTransaction(blockTime, blockNumber, transaction.nonce.hex,
                  ByteArrayUtil.convertByteArrayToDouble(transaction.value, 15, 3), transaction.sendAddress.hex,
                  transaction.receiveAddress.hex, transaction.gasPrice, transaction.gasLimit,
                  transaction.data.map(_.hex), transaction.hash.hex,
                  transaction.contractAddress.map(_.hex))
                Seq(original, original.copy(sendAddress = original.receiveAddress,
                  receiveAddress = original.sendAddress, value = original.value * -1))
            }
      }
  }

  def run(config: EthereumTransactionsPipelineConfig): Unit = {
    ioHandler.deleteFilesByPredicate(config.output.inbox.path, _ => true)
    val rawBlocks = spark
      .read
      .ethereum(config.input)
    val transactions = createTransactionsDS(rawBlocks)
    datasetStore.storeParquet(config.output.inbox, transactions, saveMode = SaveMode.Append)
  }
}

trait EthereumTransactionsPipelineComponent {
  this: SparkSessionComponent with IOHandlerComponent with DatasetStoreComponent =>

  lazy val driver: EthereumTransactionsPipeline = new EthereumTransactionsPipeline()
}