package com.endor.blockchain.ethereum.blocksummaries

import java.sql.Timestamp

import com.endor.spark.blockchain._
import org.apache.spark.sql.{Encoder, Encoders}
import org.ethereum.core.{BlockHeader => JBlockHeader, BlockSummary => JBlockSummary}
import org.ethereum.vm.DataWord

import scala.collection.JavaConverters._

final case class TokenTransactionRawValue(contractAddress: String, blockNumber: Long, fromAddress: String,
                                          toAddress: String, value: Array[Byte], transactionHash: String,
                                          transactionIndex: Int, timestamp: Timestamp)

object TokenTransactionRawValue {
  implicit val encoder: Encoder[TokenTransactionRawValue] = Encoders.product
}

final case class TokenTransaction(contractAddress: String, blockNumber: Long, fromAddress: String,
                                  toAddress: String, value: Double, transactionHash: String,
                                  transactionIndex: Int, timestamp: Timestamp)

object TokenTransaction {
  implicit val encoder: Encoder[TokenTransaction] = Encoders.product
}

final case class Transaction(timestamp: Timestamp, blockNumber: Long, nonce: String, value: Long,
                             sendAddress: String, receiveAddress: String, gasPrice: Long, gasLimit: Long,
                             data: Option[String], hash: String, contractAddress: Option[String],
                             isInternal: Boolean, gasUsed: Long, successful: Boolean, fee: Long)

object Transaction {
  implicit val encoder: Encoder[Transaction] = Encoders.product
}

final case class BlockHeader(parentHash: String, coinBase: Array[Byte], difficulty: Array[Byte], timestamp: Long,
                             number: Long, gasLimit: Long, gasUsed: Long, mixHash: Array[Byte], extraData: Array[Byte],
                             nonce: Array[Byte])
object BlockHeader {
  def fromEthereumjBlockHeader(blockHeader: JBlockHeader): BlockHeader =
    BlockHeader(blockHeader.getParentHash.hex, blockHeader.getCoinbase,
      blockHeader.getDifficulty, blockHeader.getTimestamp, blockHeader.getNumber,
      blockHeader.getGasLimit.asLong, blockHeader.getGasUsed, blockHeader.getMixHash,
      blockHeader.getExtraData, blockHeader.getNonce)
  implicit val encoder: Encoder[BlockHeader] = Encoders.product

}

final case class BlockReward(from: String, to: String, value: Long)

object BlockReward {
  implicit val encoder: Encoder[BlockReward] = Encoders.product
}

final case class BlockSummary(blockHeader: BlockHeader, ethereumTransactions: Seq[Transaction],
                              uncleHeaders: Seq[BlockHeader], erc20Transactions: Seq[TokenTransactionRawValue],
                              rewards: Seq[BlockReward])

object BlockSummary {
  implicit val encoder: Encoder[BlockSummary] = Encoders.product
  val erc20EventTopic: DataWord = new DataWord("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
  val zeroAddress: String = "0000000000000000000000000000000000000000"
  def toMWei(v: BigInt): Long = (v / 1000000).longValue()

  def fromJBlockSummary(jblockSummary: JBlockSummary): BlockSummary = {
    val block = jblockSummary.getBlock
    val transactions = extractTransactions(jblockSummary)
    val rewards = extractRewards(jblockSummary, transactions)
    val erc20Transactions = extractTokenTransactions(jblockSummary)
    val header = BlockHeader.fromEthereumjBlockHeader(block.getHeader)
    val uncleHeaders = block.getUncleList.asScala.map(BlockHeader.fromEthereumjBlockHeader)
    BlockSummary(header, transactions, uncleHeaders, erc20Transactions, rewards)
  }

  private def extractRewards(jblockSummary: JBlockSummary, transactions: Seq[Transaction]): Seq[BlockReward] = {
    val baseRewards = jblockSummary.getRewards.asScala.toMap.map {
      case (k, v) => k.hex -> BigInt(v)
    }
    val coinBase = jblockSummary.getBlock.getCoinbase.hex
    val fees = transactions
      .map(tx => tx.sendAddress -> BigInt(tx.fee))
      .toMap
    val totalFees = fees.values.reduceOption(_ + _).getOrElse(BigInt(0))
    val feeRewards = fees.map {
      case (k, v) => BlockReward(k, coinBase, toMWei(v))
    }
    baseRewards
      .map {
        case (k, v) if k == coinBase => BlockReward(zeroAddress, k, toMWei(v - totalFees))
        case (k, v) => BlockReward(zeroAddress, k, toMWei(v))
      }.toSeq ++ feeRewards
  }

  private def extractTokenTransactions(jblockSummary: JBlockSummary): Seq[TokenTransactionRawValue] = {
    val block = jblockSummary.getBlock
    val blockTime = new Timestamp(block.getTimestamp * 1000)
    val blockNumber = block.getNumber
    val txReceipts = jblockSummary.getReceipts.asScala.toList
    txReceipts
      .zipWithIndex
      .flatMap {
        case (receipt, index) =>
          val tx = receipt.getTransaction
          receipt.getLogInfoList.asScala.toList
            .collect {
              case logInfo if logInfo.getTopics.asScala.exists(_ equals erc20EventTopic) =>
                val Seq(_, fromAddress, toAddress) = logInfo.getTopics.asScala.map(_.getLast20Bytes)
                TokenTransactionRawValue(logInfo.getAddress.hex, blockNumber, fromAddress.hex, toAddress.hex, logInfo.getData,
                  tx.getHash.hex, index, blockTime)
            }
      }
  }

  private def extractTransactions(jblockSummary: JBlockSummary): Seq[Transaction] = {
    val block = jblockSummary.getBlock
    val blockTime = new Timestamp(block.getTimestamp * 1000)
    val blockNumber = block.getNumber
    val execSummaries = jblockSummary.getSummaries.asScala.toList
    execSummaries
      .flatMap(tes => {
        val internalTx = tes.getInternalTransactions.asScala.toList
          .map(it =>
            Transaction(blockTime, blockNumber, it.getNonce.hex, toMWei(it.getValue.asBigInt),
              it.getSender.hex, it.getReceiveAddress.hex, it.getGasPrice.asLong, it.getGasLimit.asLong,
              Option(it.getData).map(_.hex), it.getHash.hex, Option(it.getContractAddress).map(_.hex),
              isInternal = true, tes.getGasUsed.longValue(), !tes.isFailed, 0)
          )
        val ethereumjTx = tes.getTransaction
        val tx = Transaction(blockTime, blockNumber, ethereumjTx.getNonce.hex,
          toMWei(ethereumjTx.getValue.asBigInt), ethereumjTx.getSender.hex,
          ethereumjTx.getReceiveAddress.hex, ethereumjTx.getGasPrice.asLong, ethereumjTx.getGasLimit.asLong,
          Option(ethereumjTx.getData).map(_.hex), ethereumjTx.getHash.hex,
          Option(ethereumjTx.getContractAddress).map(_.hex), isInternal = false, tes.getGasUsed.longValue(),
          !tes.isFailed, toMWei(BigInt(tes.getFee)))
        tx :: internalTx
      })
  }
}
