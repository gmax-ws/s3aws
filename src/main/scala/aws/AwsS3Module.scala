package aws

import cats.effect.IO
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import java.io.File
import java.net.URL
import scala.jdk.CollectionConverters.ListHasAsScala

trait S3Client {
  val S3service: IO[S3Client.Service.type] = IO.pure(S3Client.Service)
}

object S3Client extends S3Client {

  sealed trait Service[F[_]] {
    def s3client(accessKeyId: String, secretAccessKey: String): F[AmazonS3]

    def createBucket(client: AmazonS3, bucketName: String): F[Bucket]

    def upload(client: AmazonS3, bucketName: String, key: String, file: File, isPublic: Boolean): F[PutObjectResult]

    def download(client: AmazonS3, bucketName: String, key: String): F[S3Object]

    def getResourceUrl(client: AmazonS3, bucketName: String, key: String): F[URL]

    def getObjectSummary(client: AmazonS3, bucketName: String, prefix: String): F[Option[S3ObjectSummary]]
  }

  object Service extends S3Client.Service[IO] {
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

  val session: IO[(S3Client.Service.type, AmazonS3)] =
    for {
      service <- S3service.S3service
      client <- service.s3client(accessKeyId, secretAccessKey)
    } yield (service, client)

  def download(bucketName: String, key: String): IO[S3Object] =
    for {
      (service, client) <- session
      obj <- service.download(client, bucketName, key)
    } yield obj

  def upload(bucketName: String, key: String, file: File, isPublic: Boolean): IO[PutObjectResult] =
    for {
      (service, client) <- session
      summary <- service.getObjectSummary(client, bucketName, key)
      _ = if (summary.isEmpty) service.createBucket(client, bucketName)
      obj <- service.upload(client, bucketName, key, file, isPublic)
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