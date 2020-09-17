package aws

import java.io.File
import java.net.URL

import cats.effect.IO
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model._

/**
 * This class is a helper used to operate on Amazon Web Services S3 storage.
 *
 * @author Marius Gligor
 */
class AwsS3Client(accessKeyId: String, secretAccessKey: String) {

  private val s3client = IO {
    val credentials = new BasicAWSCredentials(accessKeyId, secretAccessKey)
    AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))
      .build()
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
  def upload(bucketName: String, key: String, fileName: String, isPublic: Boolean): IO[PutObjectResult] = {
    s3client map { client =>
      // create bucket if not exists
      if (client.doesBucketExistV2(bucketName))
        client.createBucket(bucketName)

      val file = new File(fileName)
      val access = if (isPublic) CannedAccessControlList.PublicRead else CannedAccessControlList.Private
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
  def doesResourceExist(bucketName: String, prefix: String): IO[Option[String]] = {
    s3client map { client =>
      val objectsList = client.listObjects(bucketName, prefix).getObjectSummaries
      if (objectsList.isEmpty) None else Some(objectsList.get(0).getETag)
    }
  }
}

object AwsS3Client {
  def apply(accessKeyId: String, secretAccessKey: String) =
    new AwsS3Client(accessKeyId, secretAccessKey)
}