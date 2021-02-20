package aws

import cats.effect.IO
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import java.io.File
import java.net.URL
import scala.jdk.CollectionConverters.ListHasAsScala

trait S3Client {
  val S3service: S3Client.Service.type = S3Client.Service
}

object S3Client extends S3Client {

  sealed trait Service {
    def s3client(accessKeyId: String, secretAccessKey: String): IO[AmazonS3]

    def createBucket(client: AmazonS3, bucketName: String): IO[Bucket]

    def upload(client: AmazonS3, bucketName: String, key: String, file: File, isPublic: Boolean): IO[PutObjectResult]

    def download(client: AmazonS3, bucketName: String, key: String): IO[S3Object]

    def getResourceUrl(client: AmazonS3, bucketName: String, key: String): IO[URL]

    def getObjectSummary(client: AmazonS3, bucketName: String, prefix: String): IO[Option[S3ObjectSummary]]
  }

  object Service extends S3Client.Service {
    def s3client(accessKeyId: String, secretAccessKey: String): IO[AmazonS3] = IO {
      val credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
      val provider = new AWSStaticCredentialsProvider(credentials)
      AmazonS3ClientBuilder.standard()
        .withCredentials(provider)
        .build()
    }

    def createBucket(client: AmazonS3, bucketName: String): IO[Bucket] = IO {
      client.createBucket(bucketName)
    }

    def upload(client: AmazonS3, bucketName: String, key: String, file: File, isPublic: Boolean): IO[PutObjectResult] = IO {
      val request = new PutObjectRequest(bucketName, key, file)
        .withCannedAcl(access(isPublic))
      client.putObject(request)
    }

    def download(client: AmazonS3, bucketName: String, key: String): IO[S3Object] = IO {
      client.getObject(new GetObjectRequest(bucketName, key))
    }

    def getResourceUrl(client: AmazonS3, bucketName: String, key: String): IO[URL] =
      IO(client.getUrl(bucketName, key))

    def getObjectSummary(client: AmazonS3, bucketName: String, prefix: String): IO[Option[S3ObjectSummary]] = IO {
      client.listObjects(bucketName, prefix).getObjectSummaries.asScala.toList.headOption
    }

    private def access(isPublic: Boolean): CannedAccessControlList =
      if (isPublic) CannedAccessControlList.PublicRead else CannedAccessControlList.Private
  }

}

class S3Api(accessKeyId: String, secretAccessKey: String)(implicit S3service: S3Client) {

  val s3client: IO[AmazonS3] =
    for {
      client <- S3service.S3service.s3client(accessKeyId, secretAccessKey)
    } yield client

  def download(bucketName: String, key: String): IO[S3Object] =
    for {
      client <- s3client
      obj <- S3service.S3service.download(client, bucketName, key)
    } yield obj

  def upload(bucketName: String, key: String, file: File, isPublic: Boolean): IO[PutObjectResult] =
    for {
      client <- s3client
      summary <- S3service.S3service.getObjectSummary(client, bucketName, key)
      _ = if (summary.isEmpty) S3service.S3service.createBucket(client, bucketName)
      obj <- S3service.S3service.upload(client, bucketName, key, file, isPublic)
    } yield obj
}

object S3Api {
  def apply(accessKeyId: String, secretAccessKey: String)(implicit client: S3Client) =
    new S3Api(accessKeyId, secretAccessKey)(client)
}

object AwsS3Module extends App {
  implicit val api: S3Client.type = S3Client
  val programs = S3Api("accesskeyid", "accesskeysecret")
  val obj = programs.download("bucketname", "key").unsafeRunSync()
}