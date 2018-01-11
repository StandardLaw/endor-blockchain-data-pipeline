package com.endor.blockchain.ethereum

object ByteArrayUtil {
  def padByteArray(input: Array[Byte]): Array[Byte] =
    Array.fill[Byte](Math.max(java.lang.Long.BYTES - input.length, 1))(0) ++ input

  def convertByteArrayToDouble(input: Array[Byte], exponent: Int, decimalPrecision: Int): Double =
    (BigDecimal(BigInt(padByteArray(input)) / BigInt(Math.pow(10, exponent.toDouble).toLong)) / Math.pow(10, decimalPrecision.toDouble)).doubleValue()
}
