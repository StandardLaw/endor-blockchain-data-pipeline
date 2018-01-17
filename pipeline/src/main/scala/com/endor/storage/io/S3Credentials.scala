package com.endor.storage.io

import com.amazonaws.auth.{AWSCredentials, BasicAWSCredentials}

object S3Credentials {
  val dummyCredentials: AWSCredentials = new BasicAWSCredentials("unused_key", "unused_secret")
}
