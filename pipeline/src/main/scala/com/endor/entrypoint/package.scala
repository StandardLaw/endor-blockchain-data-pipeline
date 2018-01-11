package com.endor

import com.endor.context.Context
import org.apache.spark.sql.SparkSession
import org.slf4j.MDC

package object entrypoint {
  implicit class SparkEntryPoint(spark: SparkSession) {

    def withContext(config: EntryPointConfig)(body: => Unit)(implicit context: Context): Unit = {
      // TODO(izik): send configuration artifact

      MDC.put("sparkOperation", config.operation)
      MDC.put("customer", config.customer)
      MDC.put("executionId", config.executionId)

      config.subIdentifiers.foreach { case (key, value) =>
        MDC.put(key, value)
      }

      context.callsiteContext.initContext(
        spark.sparkContext, config.operation, config.customer, config.executionId, config.subIdentifiers
      )

      try {
        body
      } finally {
        MDC.remove("sparkOperation")
        MDC.remove("customer")
        MDC.remove("executionId")

        config.subIdentifiers.keys.foreach(MDC.remove)
      }
    }
  }
}
