package com.endor

import com.endor.context.Context
import org.apache.spark.sql.SparkSession
import org.slf4j.MDC

package object entrypoint {
  implicit class SparkEntryPoint(spark: SparkSession) {

    def withContext(config: EntryPointConfig)(body: => Unit)(implicit context: Context): Unit = {
      // TODO(izik): send configuration artifact

      MDC.put("sparkOperation", config.operation)

      context.callsiteContext.initContext(
        spark.sparkContext, config.operation
      )

      try {
        body
      } finally {
        MDC.remove("sparkOperation")
      }
    }
  }
}
