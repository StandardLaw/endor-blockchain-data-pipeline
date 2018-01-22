package com.endor.blockchain.ethereum.transaction

import java.sql.Timestamp

import com.endor.blockchain.ethereum.ByteArrayUtil
import com.endor.spark.blockchain._
import com.endor.spark.blockchain.ethereum._
import com.endor.spark.blockchain.ethereum.block.SimpleEthereumBlock
import com.endor.storage.io.IOHandler
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import play.api.libs.json.{Json, OFormat}

final case class EthereumTransactionsPipelineConfig(input: String, output: String)

object EthereumTransactionsPipelineConfig {
  implicit val format: OFormat[EthereumTransactionsPipelineConfig] = Json.format[EthereumTransactionsPipelineConfig]
}

class EthereumTransactionsPipeline(ioHandler: IOHandler)(implicit spark: SparkSession) {

  def createTransactionsDS(rawEthereum: Dataset[SimpleEthereumBlock]): Dataset[ProcessedTransaction] = {
    rawEthereum
      .flatMap {
        block =>
          val blockTime = new Timestamp(block.ethereumBlockHeader.timestamp)
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
    ioHandler.deleteFilesByPredicate(ioHandler.extractPathFromFullURI(config.output), _ => true)
    val rawBlocks = spark
      .read
      .ethereum(config.input)
    val transactions = createTransactionsDS(rawBlocks)
    transactions
      .write
      .mode(SaveMode.Append)
      .parquet(config.output)
  }
}
