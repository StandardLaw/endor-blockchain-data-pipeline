package com.endor.blockchain.ethereum.transaction

import java.sql.Timestamp

import org.apache.spark.sql.{Encoder, Encoders}

final case class ProcessedTransaction(timestamp: Timestamp, blockNumber: Long, nonce: String, value: Double,
                                      sendAddress: String, receiveAddress: String, gasPrice: Long, gasLimit: Long,
                                      data: Option[String], hash: String, contractAddress: Option[String])

object ProcessedTransaction {
  implicit val encoder: Encoder[ProcessedTransaction] = Encoders.product[ProcessedTransaction]
}
