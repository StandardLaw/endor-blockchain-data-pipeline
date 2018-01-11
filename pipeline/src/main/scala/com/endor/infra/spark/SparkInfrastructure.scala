package com.endor.infra.spark

import play.api.libs.json.{Json, OFormat}
import com.endor.serialization._
/**
  * Created by user on 05/04/17.
  */
trait SparkInfrastructure {
  def isPrimary: Boolean
}

object SparkInfrastructure {
  final case class EMR(isPrimary: Boolean) extends SparkInfrastructure
  final case class Local(isPrimary: Boolean) extends SparkInfrastructure

  implicit lazy val format: OFormat[SparkInfrastructure] = formatFor(
    "EMR" -> Json.format[EMR],
    "Local" -> Json.format[Local]
  )
}
