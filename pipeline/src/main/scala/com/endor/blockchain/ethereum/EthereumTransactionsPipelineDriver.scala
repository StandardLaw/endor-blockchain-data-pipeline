package com.endor.blockchain.ethereum

import java.sql.Timestamp

import com.endor.spark.blockchain._
import com.endor.spark.blockchain.ethereum._
import com.endor.spark.blockchain.ethereum.block.SimpleEthereumBlock
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}

final case class EthereumTransactionsPipelineDriver()
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
                  ByteArrayUtil.convertByteArrayToDouble(transaction.value, 15), transaction.sendAddress.hex,
                  transaction.receiveAddress.hex, transaction.gasPrice, transaction.gasLimit,
                  transaction.data.map(_.hex), transaction.hash.hex,
                  transaction.contractAddress.map(_.hex))
            }
      }
  }

  def start(): Unit = {
    val rawBlocks = spark
      .readStream
      .ethereum(ethereumSourcePath)
    val transactions = createTransactionsDS(rawBlocks)
    transactions
      .writeStream
      .option("checkpointLocation", transactionsCheckpointPath)
      .format("parquet")
      .outputMode(OutputMode.Append())
      .start(transactionsOutputPath)
      .awaitTermination()
  }
}
