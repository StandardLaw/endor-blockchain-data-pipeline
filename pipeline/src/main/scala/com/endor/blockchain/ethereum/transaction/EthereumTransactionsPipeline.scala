package com.endor.blockchain.ethereum.transaction

import java.sql.Timestamp

import com.endor.blockchain.ethereum.{ByteArrayUtil, ProcessedTransaction}
import com.endor.spark.blockchain._
import com.endor.spark.blockchain.ethereum._
import com.endor.spark.blockchain.ethereum.block.SimpleEthereumBlock
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

final case class EthereumTransactionsPipelineConfig(input: String, output: String)

final case class EthereumTransactionsPipeline()
                                             (implicit spark: SparkSession) {

  def createTransactionsDS(rawEthereum: Dataset[SimpleEthereumBlock]): Dataset[ProcessedTransaction] = {
    rawEthereum
      .flatMap {
        block =>
          val blockTime = new Timestamp(block.ethereumBlockHeader.timestamp)
          val blockNumber = block.ethereumBlockHeader.number
          block.ethereumTransactions
            .map(_.toEnriched)
            .map {
              transaction =>
                ProcessedTransaction(blockTime, blockNumber, transaction.nonce.hex,
                  ByteArrayUtil.convertByteArrayToDouble(transaction.value, 15, 3), transaction.sendAddress.hex,
                  transaction.receiveAddress.hex, transaction.gasPrice, transaction.gasLimit,
                  transaction.data.map(_.hex), transaction.hash.hex,
                  transaction.contractAddress.map(_.hex))
            }
      }
  }

  def run(config: EthereumTransactionsPipelineConfig): Unit = {
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
