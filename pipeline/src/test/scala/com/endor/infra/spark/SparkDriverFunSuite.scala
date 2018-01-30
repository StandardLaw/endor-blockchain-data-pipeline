package com.endor.infra.spark

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.{BeforeAndAfterEachTestData, Matchers, TestData}

import scala.util.Random

/**
  * Created by user on 14/05/17.
  */
trait SparkDriverFunSuite extends SharedSQLContext with Matchers with BeforeAndAfterEachTestData {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var _randomSeed: Long = Random.nextLong()
  protected def randomSeed: Long = _randomSeed
  protected val randomGenerator: Random = new Random(randomSeed)

  override protected def afterEach(testData: TestData): Unit = super.afterEach(testData)
  override protected def beforeEach(testData: TestData): Unit = {
    _randomSeed = Random.nextLong()
    randomGenerator.setSeed(_randomSeed)
  }

  final def randomString(length: Int): String = randomGenerator.alphanumeric.take(length).mkString

  final def randomDate(minDate: Timestamp, maxDate: Timestamp): Timestamp = {
    val offset = minDate.getTime
    val end = maxDate.getTime
    val diff = end - offset + 1
    new Timestamp(offset + (randomGenerator.nextDouble() * diff).toLong)
  }

  final def randomDate(): Timestamp = {
    randomDate(new Timestamp(0L), new Timestamp(Long.MaxValue))
  }
}

trait IsolatedSparkDriverFunSuite extends SparkDriverFunSuite {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected var _isolatedSpark: SparkSession = super.spark
  override def spark: SparkSession = _isolatedSpark

  override protected def beforeEach(testData: TestData): Unit = {
    _isolatedSpark = super.spark.newSession()
    super.beforeEach(testData)
  }
}
