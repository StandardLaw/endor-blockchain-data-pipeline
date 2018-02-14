package com.endor.infra

import org.apache.spark.sql.SparkSession

trait SparkSessionComponent {
  implicit def spark: SparkSession
}
