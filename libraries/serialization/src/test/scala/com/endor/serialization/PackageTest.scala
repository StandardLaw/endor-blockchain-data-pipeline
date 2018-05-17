package com.endor.serialization

import java.sql.Timestamp
import java.time.Instant

import org.scalatest.{FunSuite, Matchers}
import play.api.libs.json.Json

class PackageTest extends FunSuite with Matchers {
  test("serde java.sql.Timestamp") {
    val ts = Timestamp.from(Instant.now())
    val jsonString = Json.prettyPrint(Json.toJson(ts))
    println(jsonString)
    Json.parse(jsonString).as[Timestamp] should equal(ts)
  }
}
