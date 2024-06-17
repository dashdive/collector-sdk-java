package com.ddtest;

import com.dashdive.Dashdive;
import com.dashdive.ImmutableS3EventAttributes;
import com.dashdive.S3EventAttributeExtractorFactory;
import com.dashdive.internal.DashdiveConnection;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/*
 * We test the following cases:
 * - Bucket in same region (us-west-2)
 * - Bucket in different region (us-east-1)
 * - Malformed request dispatched to S3Client
 * - Handling multithreaded calls to same S3Client
 * - With and without an explicit `close()` call to Dashdive instance
 */

public class Main {
  private static final Logger logger = LoggerFactory.getLogger(Main.class);

  private static final String SAME_REGION_BUCKET_NAME_1 = "ddsdk-integration-sameregion-1";
  private static final String SAME_REGION_BUCKET_NAME_2 = "ddsdk-integration-sameregion-2";
  private static final Region SAME_REGION_BUCKET_REGION = Region.US_WEST_2;

  private static final String DIFFERENT_REGION_BUCKET_NAME = "ddsdk-integration-diffregion";
  private static final Region DIFFERENT_REGION_BUCKET_REGION = Region.US_EAST_2;

  private static final String NONEXISTENT_BUCKET_NAME = "my-nonexistent-bucket-2983501289356";
  private static final String NONEXISTENT_KEY = "nonexistent-key";

  // We can't have an S3Client that handles buckets in multiple regions. See:
  // https://github.com/aws/aws-sdk-java-v2/issues/52#issuecomment-2018573991

  /*
  AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id) \
  AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key) \
  gradle :integration_test:client:run
  */

  public static void main(String[] args) {
    DashdiveConnection._setIngestBaseUri(URI.create("http://127.0.0.1:3232"));

    final String API_KEY = "TESTKEYZ.yiXYc+TQZaJvcNq80KA7S6+eAfCPsW4kR59ooVAu3pj/Eqjo";
    final Dashdive dashdive =
        Dashdive.builder()
            .apiKey(API_KEY)
            .s3EventAttributeExtractorFactory(
                S3EventAttributeExtractorFactory.from(
                    (input) ->
                        ImmutableS3EventAttributes.builder().customerId("dummy-customer").build()))
            .build();

    // Assumes credentials are set in env before run
    final String accessKeyId = System.getenv("AWS_ACCESS_KEY_ID");
    final String secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY");
    final S3Client s3ClientSameRegion =
        dashdive
            .withInstrumentation(S3Client.builder())
            // We assume this EC2 instance is in the same region as the one specified here
            .region(SAME_REGION_BUCKET_REGION)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
            .build();

    final S3Client s3ClientDifferentRegion =
        dashdive
            .withInstrumentation(S3Client.builder())
            .region(DIFFERENT_REGION_BUCKET_REGION)
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKeyId, secretAccessKey)))
            .build();

    final Runnable sameRegionBucketTask1 =
        () ->
            BucketTester.doTestBucketActions(
                s3ClientSameRegion, SAME_REGION_BUCKET_NAME_1, SAME_REGION_BUCKET_REGION);
    final Runnable sameRegionBucketTask2 =
        () ->
            BucketTester.doTestBucketActions(
                s3ClientSameRegion, SAME_REGION_BUCKET_NAME_2, SAME_REGION_BUCKET_REGION);
    final Runnable differentRegionBucketTask =
        () ->
            BucketTester.doTestBucketActions(
                s3ClientDifferentRegion,
                DIFFERENT_REGION_BUCKET_NAME,
                DIFFERENT_REGION_BUCKET_REGION);

    final Thread sameRegionBucketThread1 = new Thread(sameRegionBucketTask1);
    final Thread sameRegionBucketThread2 = new Thread(sameRegionBucketTask2);
    final Thread differentRegionBucketThread = new Thread(differentRegionBucketTask);

    sameRegionBucketThread1.start();
    sameRegionBucketThread2.start();
    differentRegionBucketThread.start();

    try {
      final GetObjectRequest request =
          GetObjectRequest.builder().bucket(NONEXISTENT_BUCKET_NAME).key(NONEXISTENT_KEY).build();
      s3ClientSameRegion.getObject(request);
    } catch (NoSuchBucketException exception) {
      logger.info("Encountered anticipated S3Exception: {}", exception.getMessage());
    }

    try {
      sameRegionBucketThread1.join();
      sameRegionBucketThread2.join();
      differentRegionBucketThread.join();
    } catch (InterruptedException interrupt) {
      logger.error("UNEXPECTED interrupt", interrupt);
    }

    s3ClientSameRegion.close();
    s3ClientDifferentRegion.close();
    dashdive.close();
  }
}

class BucketTester {
  private static final Logger logger = LoggerFactory.getLogger(Dashdive.class);

  private static final String objectKey1 = "java_payload1.txt";
  private static final String objectKey2 = "java_payload2.txt";
  private static final String multipartObjectKey = "multipart.txt";

  private static final String objectContent1 = "abc";
  private static final String objectContent2 = "wxyz";
  private static final String multipartFinalContents = "END";

  private static final String expectedCombinedContent =
      (objectContent1 + objectContent2).toUpperCase();

  private BucketTester() {}

  public static void doTestBucketActions(
      S3Client s3Client, String bucketName, Region bucketRegion) {
    try {
      doTestBucketActionsUnsafe(s3Client, bucketName, bucketRegion);
    } catch (Exception s3Exception) {
      logger.error(
          "[Bucket {}, {}] Encountered UNEXPECTED exception",
          bucketName,
          bucketRegion.toString(),
          s3Exception);
    }
  }

  private static void doTestBucketActionsUnsafe(
      S3Client s3Client, String bucketName, Region bucketRegion) {
    if (!doesBucketExist(s3Client, bucketName)) {
      s3Client.createBucket(
          CreateBucketRequest.builder()
              .bucket(bucketName)
              .createBucketConfiguration(
                  CreateBucketConfiguration.builder().locationConstraint(bucketRegion.id()).build())
              .build());
      logger.info("[Bucket {}, {}] Created bucket", bucketName, bucketRegion.toString());
    } else {
      logger.info("[Bucket {}, {}] Bucket already exists", bucketName, bucketRegion.toString());
    }

    uploadString(s3Client, bucketName, objectKey1, objectContent1);
    uploadString(s3Client, bucketName, objectKey2, objectContent2);
    logger.info("[Bucket {}, {}] Uploaded objects", bucketName, bucketRegion.toString());

    final String downloadedContent1 = downloadString(s3Client, bucketName, objectKey1);
    final String downloadedContent2 = downloadString(s3Client, bucketName, objectKey2);
    final String combinedContent = (downloadedContent1 + downloadedContent2).toUpperCase();
    if (!combinedContent.equals(expectedCombinedContent)) {
      logger.error(
          "[Bucket {}, {}] Downloaded objects MISMATCH: expected '{}', got '{}'",
          bucketName,
          bucketRegion.toString(),
          expectedCombinedContent,
          combinedContent);
    } else {
      logger.info(
          "[Bucket {}, {}] Downloaded objects match expected values",
          bucketName,
          bucketRegion.toString());
    }

    deleteObject(s3Client, bucketName, objectKey1);
    deleteObject(s3Client, bucketName, objectKey2);
    logger.info("[Bucket {}, {}] Deleted objects", bucketName, bucketRegion.toString());

    NoSuchKeyException anticipatedException = null;
    try {
      downloadString(s3Client, bucketName, objectKey1);
    } catch (NoSuchKeyException encounteredException) {
      anticipatedException = encounteredException;
    }

    if (anticipatedException == null) {
      logger.error(
          "[Bucket {}, {}] Missing expected NoSuchKeyException when deleting non-existent object",
          bucketName,
          bucketRegion.toString());
    } else {
      logger.info(
          "[Bucket {}, {}] Encountered expected NoSuchKeyException when deleting non-existent"
              + " object",
          bucketName,
          bucketRegion.toString());
    }

    final Path multipartContents = Path.of(ClassLoader.getSystemClassLoader().getResource("multipart.txt").getPath());
    uploadStringsInParts(
        s3Client, bucketName, multipartObjectKey, multipartContents, multipartFinalContents);
    final String downloadedMultipartContent =
        downloadString(s3Client, bucketName, multipartObjectKey);
    final boolean hasCorrectEnd = downloadedMultipartContent.endsWith(multipartFinalContents);
    // `multipart.txt` has 211,000 lines of "abcdefghijklmnopqrstuvwxyz", plus newlines
    final boolean hasCorrectLength =
        downloadedMultipartContent.length() == 211_000 * 27 + multipartFinalContents.length();
    if (!hasCorrectEnd || !hasCorrectLength) {
      logger.error(
          "[Bucket {}, {}] Downloaded multipart object MISMATCH: hasCorrectEnd={},"
              + " hasCorrectLength={}",
          bucketName,
          bucketRegion.toString(),
          hasCorrectEnd,
          hasCorrectLength);
    } else {
      logger.info(
          "[Bucket {}, {}] Downloaded multipart object matches expected values",
          bucketName,
          bucketRegion.toString());
    }
    deleteObject(s3Client, bucketName, multipartObjectKey);

    deleteBucket(s3Client, bucketName);
    logger.info("[Bucket {}, {}] Deleted bucket", bucketName, bucketRegion.toString());
  }

  private static boolean doesBucketExist(S3Client s3Client, String bucketName) {
    try {
      final HeadBucketResponse headBucketResponse =
          s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
      return headBucketResponse.sdkHttpResponse().isSuccessful();
    } catch (S3Exception exception) {
      return false;
    }
  }

  private static PutObjectResponse uploadString(
      S3Client s3Client, String bucketName, String objectKey, String content) {
    return s3Client.putObject(
        PutObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
        RequestBody.fromString(content));
  }

  private static CompleteMultipartUploadResponse uploadStringsInParts(
      S3Client s3Client,
      String bucketName,
      String objectKey,
      Path mainContent,
      String finalContent) {
    final CreateMultipartUploadResponse createMultipartResponse =
        s3Client.createMultipartUpload(
            CreateMultipartUploadRequest.builder().bucket(bucketName).key(objectKey).build());
    final String uploadId = createMultipartResponse.uploadId();

    final List<RequestBody> contents = new ArrayList<>();
    contents.add(RequestBody.fromFile(mainContent));
    contents.add(RequestBody.fromString(finalContent));

    final List<CompletedPart> completedParts = new ArrayList<>();
    for (int i = 0; i < contents.size(); i++) {
      final UploadPartResponse response =
          s3Client.uploadPart(
              UploadPartRequest.builder()
                  .bucket(bucketName)
                  .key(objectKey)
                  .uploadId(uploadId)
                  .partNumber(i + 1)
                  .build(),
              contents.get(i));
      completedParts.add(CompletedPart.builder().partNumber(i + 1).eTag(response.eTag()).build());
    }

    return s3Client.completeMultipartUpload(
        CompleteMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(objectKey)
            .uploadId(uploadId)
            .multipartUpload(CompletedMultipartUpload.builder().parts(completedParts).build())
            .build());
  }

  private static String downloadString(S3Client s3Client, String bucketName, String objectKey) {
    return s3Client
        .getObject(
            GetObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
            ResponseTransformer.toBytes())
        .asUtf8String();
  }

  private static DeleteObjectResponse deleteObject(
      S3Client s3Client, String bucketName, String objectKey) {
    return s3Client.deleteObject(
        DeleteObjectRequest.builder().bucket(bucketName).key(objectKey).build());
  }

  private static DeleteBucketResponse deleteBucket(S3Client s3Client, String bucketName) {
    return s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
  }
}
