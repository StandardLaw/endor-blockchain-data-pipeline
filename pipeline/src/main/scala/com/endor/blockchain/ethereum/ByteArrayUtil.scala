package com.endor.blockchain.ethereum

object ByteArrayUtil {
  def padByteArray(input: Array[Byte]): Array[Byte] =
    Array.fill[Byte](Math.max(java.lang.Long.BYTES - input.length, 1))(0) ++ input

  def convertByteArrayToDouble(input: Array[Byte], exponent: Int): Double =
    (BigDecimal(BigInt(padByteArray(input))) / Math.pow(10, exponent.toDouble)).doubleValue()
}
