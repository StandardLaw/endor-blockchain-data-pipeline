package com.endor.serialization

import play.api.libs.json._

/**
  * Created by user on 01/12/16.
  */
final case class SerializableADTObject[T](typeName: String, instance: T) extends SerializableADT[T] {
  val reads: PartialFunction[JsValue, JsResult[T]] = {
    case obj: JsObject if (obj \ "typeName").as[String] == typeName =>
      JsSuccess(instance)
  }

  val writes: PartialFunction[T, JsObject] = {
    case obj if obj == instance =>
      JsObject(Map("typeName" -> JsString(typeName)))
  }
}
