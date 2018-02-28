package com.endor.blockchain.ethereum.ratesaggregation

import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.types.{DataTypes, StructType}

class CloseRateAggregator extends RatesAggregator {
  override def bufferSchema: StructType = new StructType()
    .add("maxDate", DataTypes.TimestampType)
    .add("rate", DataTypes.DoubleType)

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, None)
    buffer.update(1, 0.0)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val newTimestamp = input.getAs[Timestamp](0)
    if (Option(buffer.getAs[Timestamp](0)).forall(_.before(newTimestamp))) {
      buffer.update(0, newTimestamp)
      buffer.update(1, input.getAs[Double](1))
    }
  }
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val selectedBuffer = Seq(
      Option(buffer1.getAs[Timestamp](0)).map(ts => (ts, buffer1.getAs[Double](1))),
      Option(buffer2.getAs[Timestamp](0)).map(ts => (ts, buffer2.getAs[Double](1)))
    )
      .flatten
      .maxBy(_._1.getTime)
    buffer1.update(0, selectedBuffer._1)
    buffer1.update(1, selectedBuffer._2)
  }

  override def evaluate(buffer: Row): Any = buffer.getAs[Double](1)
}
