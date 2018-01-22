package com.endor.blockchain.ethereum

object ByteArrayUtil {
  def padByteArray(input: Array[Byte]): Array[Byte] =
    Array.fill[Byte](Math.max(java.lang.Long.BYTES - input.length, 1))(0) ++ input

  def convertByteArrayToDouble(input: Array[Byte], exponent: Int, decimalPrecision: Int): Double = {
    val cutOffBigInt = BigInt(Math.pow(10.0, exponent.toDouble).toLong)
    val inputBigInt = BigInt(padByteArray(input))
    (BigDecimal(inputBigInt / cutOffBigInt) / Math.pow(10.0, decimalPrecision.toDouble)).doubleValue()
  }
}
