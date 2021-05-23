package aws

import cats.Monad
import cats.effect.IO
import cats.implicits._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import java.io.File
import java.net.URL
import scala.jdk.CollectionConverters.ListHasAsScala

sealed trait S3Algebra[F[_]] {
  def s3client(accessKeyId: String, secretAccessKey: String): F[AmazonS3]

  def createBucket(client: AmazonS3, bucketName: String): F[Bucket]

  def upload(
      client: AmazonS3,
      bucketName: String,
      key: String,
      file: File,
      isPublic: Boolean
  ): F[PutObjectResult]

  def download(client: AmazonS3, bucketName: String, key: String): F[S3Object]

  def getResourceUrl(client: AmazonS3, bucketName: String, key: String): F[URL]

  def getObjectSummary(
      client: AmazonS3,
      bucketName: String,
      prefix: String
  ): F[Option[S3ObjectSummary]]
}

case class S3Programs[F[_]: Monad](
    accessKeyId: String,
    secretAccessKey: String
)(implicit S3: S3Algebra[F]) {

  val s3client: F[AmazonS3] =
    for {
      client <- S3.s3client(accessKeyId, secretAccessKey)
    } yield client

  def download(bucketName: String, key: String): F[S3Object] =
    for {
      client <- s3client
      obj <- S3.download(client, bucketName, key)
    } yield obj

  def upload(
      bucketName: String,
      key: String,
      file: File,
      isPublic: Boolean
  ): F[PutObjectResult] =
    for {
      client <- s3client
      summary <- S3.getObjectSummary(client, bucketName, key)
      _ = if (summary.isEmpty) S3.createBucket(client, bucketName)
      obj <- S3.upload(client, bucketName, key, file, isPublic)
    } yield obj
}

object S3Interpreter {

  implicit object S3CatsIOAlgebra extends S3Algebra[IO] {
    def s3client(accessKeyId: String, secretAccessKey: String): IO[AmazonS3] =
      IO {
        val credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
        val provider = new AWSStaticCredentialsProvider(credentials)
        AmazonS3ClientBuilder
          .standard()
          .withCredentials(provider)
          .build()
      }

    def createBucket(client: AmazonS3, bucketName: String): IO[Bucket] =
      IO {
        client.createBucket(bucketName)
      }

    def upload(
        client: AmazonS3,
        bucketName: String,
        key: String,
        file: File,
        isPublic: Boolean
    ): IO[PutObjectResult] =
      IO {
        val request = new PutObjectRequest(bucketName, key, file)
          .withCannedAcl(access(isPublic))
        client.putObject(request)
      }

    def download(
        client: AmazonS3,
        bucketName: String,
        key: String
    ): IO[S3Object] =
      IO {
        client.getObject(new GetObjectRequest(bucketName, key))
      }

    def getResourceUrl(
        client: AmazonS3,
        bucketName: String,
        key: String
    ): IO[URL] =
      IO(client.getUrl(bucketName, key))

    def getObjectSummary(
        client: AmazonS3,
        bucketName: String,
        prefix: String
    ): IO[Option[S3ObjectSummary]] =
      IO {
        client
          .listObjects(bucketName, prefix)
          .getObjectSummaries
          .asScala
          .toList
          .headOption
      }

    private def access(isPublic: Boolean): CannedAccessControlList =
      if (isPublic) CannedAccessControlList.PublicRead
      else CannedAccessControlList.Private
  }
}

object AwsS3Tagless extends App {

  import S3Interpreter._

  val programs = S3Programs[IO]("accesskeyid", "accesskeysecret")
  val obj = programs.download("bucketname", "key").unsafeRunSync()
}
