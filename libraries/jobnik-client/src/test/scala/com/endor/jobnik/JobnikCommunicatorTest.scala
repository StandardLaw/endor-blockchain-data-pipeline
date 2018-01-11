package com.endor.jobnik

import com.endor.artifacts.elasticsearch.ElasticsearchProxy
import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.{JsObject, JsString}

import scala.collection.mutable

/**
  * Created by izik on 19/06/2016.
  */
class JobnikCommunicatorTest extends FunSuite with Matchers {
  test("should send the write jobnik integration message format") {
    val buffer = mutable.Buffer[String]()
    @SuppressWarnings(Array("org.wartremover.warts.Serializable"))
    val jobnikCommunicator = new JobnikCommunicator((_: String, jsonMessage: String) => {
      buffer += jsonMessage
      mutable.Buffer("aa")
    }, ElasticsearchProxy.foundProxy)

    implicit val jobnikSession: Option[JobnikSession] = Option(JobnikSession("jobnik-role", JsObject(Seq(("a", JsString("aaa"))))))
    jobnikCommunicator.sendProgressIndication("driver", 2)
    buffer.length should be (1)
  }
}
