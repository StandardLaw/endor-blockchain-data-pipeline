package com.endor.blockchain.ethereum.tokens

import java.sql.Timestamp

import org.apache.spark.sql.{Encoder, Encoders}

final case class ProcessedTokenTransaction(contractAddress: String, blockNumber: Long, fromAddress: String,
                                           toAddress: String, value: Double, transactionHash: String,
                                           transactionIndex: Int, timestamp: Timestamp)

object ProcessedTokenTransaction {
  implicit val encoder: Encoder[ProcessedTokenTransaction] = Encoders.product[ProcessedTokenTransaction]
}
