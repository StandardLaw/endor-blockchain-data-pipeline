package com.endor

import org.apache.spark.sql.Encoder
import play.api.libs.functional.syntax._
import play.api.libs.json._

private object DomainTypes {
  def domainObjectIdFormatter[T <: Any { def id: String }](apply: String => T): Format[T] = new Format[T] {
    override def writes(o: T): JsValue = JsString(o.id)
    override def reads(json: JsValue): JsResult[T] = json match {
      case JsString(id) => JsSuccess(apply(id))
      case _ => JsError()
    }
  }
}

final case class CustomerId(id: String) extends AnyVal

object CustomerId {
  implicit lazy val format: Format[CustomerId] = DomainTypes.domainObjectIdFormatter(CustomerId.apply)
}

final case class DataId[T: Encoder](id: String)

object DataId {
  implicit def format[T: Encoder]: Format[DataId[T]] = DomainTypes.domainObjectIdFormatter(DataId.apply[T])
}

final case class DataKey[T: Encoder](customer: CustomerId, id: DataId[T])

object DataKey {
  implicit def format[T](implicit encoder: Encoder[T]): OFormat[DataKey[T]] = (
    (__ \ "customer").format[CustomerId] and
      (__ \ "dataId").format[DataId[T]]
    )(DataKey.apply[T], unlift(DataKey.unapply))
}