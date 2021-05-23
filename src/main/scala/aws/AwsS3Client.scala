package aws

import java.io.File
import java.net.URL
import cats.effect.IO
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model._
import com.typesafe.config.ConfigFactory

/**
  * This class is a helper used to operate on Amazon Web Services S3 storage.
  *
  * @author Marius Gligor
  */
case class AwsS3Client(
    accessKeyId: String,
    secretAccessKey: String,
    endpoint: String,
    region: String,
    local: Boolean = false
) {

  private val s3client = IO {
    val credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
    val cli = AmazonS3ClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))

    if (local)
      cli
        .withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(endpoint, region)
        )

    cli.build()
  }

  def createBucket(bucketName: String): IO[Any] = {
    s3client map { client =>
      // create bucket if not exists
      if (!client.doesBucketExistV2(bucketName))
        client.createBucket(bucketName)
    }
  }

  /**
    * Upload a file to S3.
    *
    * @param bucketName Bucket name.
    * @param key        S3 key (file path in bucket).
    * @param fileName   File name and path to upload.
    * @param isPublic   <code>true</code> if public access <code>false</code> for private access.
    * @return upload result
    */
  def upload(
      bucketName: String,
      key: String,
      fileName: String,
      isPublic: Boolean = false
  ): IO[PutObjectResult] = {
    s3client map { client =>
      // create bucket if not exists
      if (!client.doesBucketExistV2(bucketName))
        client.createBucket(bucketName)

      val file = new File(fileName)
      val access =
        if (isPublic) CannedAccessControlList.PublicRead
        else CannedAccessControlList.Private
      val request = new PutObjectRequest(bucketName, key, file)
        .withCannedAcl(access)
      client.putObject(request)
    }
  }

  /**
    * Download S3 Object.
    *
    * @param bucketName S3 bucket name.
    * @param key        S3 key (file path in bucket).
    * @return S3 Object.
    */
  def download(bucketName: String, key: String): IO[S3Object] = {
    s3client map { client =>
      val request = new GetObjectRequest(bucketName, key)
      client.getObject(request)
    }
  }

  /**
    * Get S3 resource URL.
    *
    * @param bucketName S3 bucket.
    * @param key        Bucket key
    * @return S3 resource URL
    */
  def getResourceUrl(bucketName: String, key: String): IO[URL] =
    s3client map { client =>
      client.getUrl(bucketName, key)
    }

  /**
    * Check if s S3 resource exists.
    *
    * @param bucketName S3 bucket.
    * @param prefix     keys prefix
    * @return Some[ETag] if resource exists None otherwise.
    */
  def doesResourceExist(
      bucketName: String,
      prefix: String
  ): IO[Option[String]] = {
    s3client map { client =>
      val objectsList =
        client.listObjects(bucketName, prefix).getObjectSummaries
      if (objectsList.isEmpty) None else Some(objectsList.get(0).getETag)
    }
  }
}

object AwsS3Client extends App {
  val cfg = ConfigFactory.load.getConfig("aws")
  val key = cfg.getString("key")
  val secret = cfg.getString("secret")
  val endpoint = cfg.getString("local.endpoint")
  val region = cfg.getString("local.region")
  val local = cfg.getBoolean("local.enabled")
  val aws = AwsS3Client(key, secret, endpoint, region, local)
  aws.upload("gmax", "files", "build.sbt", isPublic = true).unsafeRunSync()
}
