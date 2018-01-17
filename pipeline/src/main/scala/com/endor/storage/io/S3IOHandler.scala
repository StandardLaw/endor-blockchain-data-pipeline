package com.endor.storage.io

import java.io.{File, InputStream}

import com.amazonaws.services.s3.model.{AmazonS3Exception, CopyObjectRequest, ObjectMetadata, PutObjectRequest, _}
import com.amazonaws.services.s3.transfer.{TransferManager, TransferManagerBuilder}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.endor._
import com.endor.infra.spark.SparkInfrastructure

import scala.collection.JavaConverters._

/**
  * Created by user on 5/19/16.
  */


class S3IOHandler(sparkInfrastructure: SparkInfrastructure) extends IOHandler {
  private val s3Client: AmazonS3 = AmazonS3ClientBuilder.defaultClient()
  private lazy val pathScheme: String = sparkInfrastructure match {
    case _: SparkInfrastructure.EMR => "s3://"
    case _: SparkInfrastructure.Local => "s3n://"
  }
  private def createObjectMetadata(): ObjectMetadata = {
    val objectMetadata = new ObjectMetadata()
    objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
    objectMetadata
  }

  private def createPathFor(prefix: String, path: String): String = s"$pathScheme$prefix$path"
  private def createPathFor(path: String): String = createPathFor("", path)

  override def getReadPathFor(path: String): String = createPathFor(path)

  override def getWritePathFor(path: String): String = {
    val prefix = if(sparkInfrastructure.isPrimary) "" else "secondary-"
    createPathFor(prefix, path)
  }

  override def saveRawData(file: File, path: String): Unit = {
    val (bucketName, keyPath) = getBucketAndPath(path)
    val putObjectRequest = new PutObjectRequest(bucketName, keyPath, file)
      .withMetadata(createObjectMetadata())
    s3Client.putObject(putObjectRequest)
  }

  override def saveRawData(input: InputStream, path: String): Unit = {
    val (bucketName, keyPath) = getBucketAndPath(path)
    val putObjectRequest = new PutObjectRequest(bucketName, keyPath, input, createObjectMetadata())
    s3Client.putObject(putObjectRequest)
  }

  override def loadRawData(path: String): InputStream = {
    val (bucketName, keyPath) = getBucketAndPath(path)
    val remoteObject = s3Client.getObject(bucketName, keyPath)
    //noinspection JavaAccessorMethodCalledAsEmptyParen
    remoteObject.getObjectContent()
  }

  def getBucketAndPath(path: String): (String, String) = {
    val parts = path.split("/")
    (parts.head, parts.tail.mkString("/"))
  }

  override def pathExists(path: String): Boolean = {
    val (bucketName, keyPath) = getBucketAndPath(path)
    try
    {
      s3Client.getObjectMetadata(bucketName, keyPath)
      true
    }
    catch {
      case ex: AmazonS3Exception =>
        if (ex.getMessage === "The specified key does not exist" || ex.getStatusCode === 404)
          false
        else
          throw ex

      case ex : Exception => throw ex
    }
  }

  override def listFiles(path: String): List[FileInfo] = {
    val pathRegex = "(.*?)/(.*)".r

    val objectSummaries: Seq[S3ObjectSummary] = path match {
      case pathRegex(bucket, prefix) => s3Client.listObjectsV2(bucket, prefix).getObjectSummaries.asScala
      case _ => Nil
    }

    objectSummaries
      .map(obj => FileInfo(s"${obj.getBucketName}/${obj.getKey}", obj.getSize))
      .toList
  }

  override def extractPathFromFullURI(fullURI: String): String = {
    val pathRegex = "s3[an]?://([A-Z\\d]+:[^@]+@)?([\\w\\d/\\-\\.]*[\\w\\d])(/?\\*?)".r
    fullURI match {
      case pathRegex(_, path, _) => path
      case _ => fullURI
    }
  }

  override def deleteFilesByPredicate(path: String, predicate: (String) => Boolean): Unit = {
    val pathRegex = "(.*?)/(.*)".r
    path match {
      case pathRegex(bucket, relativePath) if sparkInfrastructure.isPrimary =>
        val filesToDelete = s3Client.listObjectsV2(bucket, relativePath)
          .getObjectSummaries.asScala
          .map(_.getKey)
          .filter(predicate)
        if(filesToDelete.nonEmpty) {
          s3Client.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(filesToDelete: _*))
        }
      case _ =>
    }
  }

  override def moveFile(sourceFilePath: String, destFilePath: String): Unit = {
    val (sourceBucket, sourceKey) = getBucketAndPath(sourceFilePath)
    val (destBucket, destKey) = getBucketAndPath(destFilePath)

    val transferManager: TransferManager = TransferManagerBuilder.defaultTransferManager
    val objectMetadata = new ObjectMetadata()
    objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
    val request = new CopyObjectRequest(sourceBucket, sourceKey, destBucket, destKey)
      .withNewObjectMetadata(objectMetadata)
    transferManager.copy(request).waitForCopyResult()
  }
}

