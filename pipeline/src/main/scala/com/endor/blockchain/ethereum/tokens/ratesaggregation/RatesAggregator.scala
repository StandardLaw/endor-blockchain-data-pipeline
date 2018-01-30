package com.endor.blockchain.ethereum.tokens.ratesaggregation

import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

abstract class RatesAggregator() extends UserDefinedAggregateFunction {
  override final def inputSchema: StructType = new StructType()
    .add("ts", DataTypes.TimestampType)
    .add("rate", DataTypes.DoubleType)

  override final def dataType: DataType = DataTypes.DoubleType
  override final def deterministic: Boolean = true
}