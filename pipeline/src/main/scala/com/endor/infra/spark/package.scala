package com.endor.infra

import com.endor.context.Context
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS

package object spark {
  def setShufflePartitions(requestedShufflePartitions: Long)
                          (implicit context: Context, sparkSession: SparkSession): Unit = {
    val shufflePartitions = if(context.testMode) {
      sparkSession.conf.get(SHUFFLE_PARTITIONS.key).toLong
    } else {
      requestedShufflePartitions
    }
    sparkSession.conf.set(SHUFFLE_PARTITIONS.key, shufflePartitions)
  }
}
