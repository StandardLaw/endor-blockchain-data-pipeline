package com.endor.infra.spark

import java.io.File
import java.nio.file.{Path, Paths}
import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.{BeforeAndAfterEachTestData, Matchers, Suite, TestData}

import scala.collection.mutable
import scala.util.Random

/**
  * Created by user on 14/05/17.
  */
trait SparkDriverSuite extends SharedSparkSession with Matchers with BeforeAndAfterEachTestData {
  this: Suite =>

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private var _randomSeed: Long = Random.nextLong()
  protected def randomSeed: Long = _randomSeed
  protected val randomGenerator: Random = new Random(randomSeed)
  private val tempDirs: mutable.ListBuffer[File] = mutable.ListBuffer.empty

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  override protected def afterEach(testData: TestData): Unit = {
    tempDirs.foreach(deleteRecursively)
    super.afterEach(testData)
  }
  override protected def beforeEach(testData: TestData): Unit = {
    _randomSeed = Random.nextLong()
    randomGenerator.setSeed(_randomSeed)
    tempDirs.clear()
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

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  final def createTempDir(tmpName: String): String = {
    val tmpDir = Paths.get(System.getProperty("java.io.tmpdir"))
    val name: Path = tmpDir.getFileSystem.getPath(tmpName)
    if (name.getParent != null) throw new IllegalArgumentException("Invalid prefix or suffix")
    val newTempDir = tmpDir.resolve(name)
    tempDirs += newTempDir.toFile
    newTempDir.toString
  }
}

trait IsolatedSparkDriverSuite extends SparkDriverSuite {
  this: Suite =>

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  protected var _isolatedSpark: SparkSession = super.spark
  override def spark: SparkSession = _isolatedSpark

  override protected def beforeEach(testData: TestData): Unit = {
    _isolatedSpark = super.spark.newSession()
    super.beforeEach(testData)
  }
}
