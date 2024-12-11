package com.dashdive;

import com.dashdive.internal.ConnectionUtils;
import com.dashdive.internal.ImmutableDashdiveInstanceInfo;
import com.dashdive.internal.ImmutableSetupDefaults;
import com.dashdive.internal.SetupDefaults;
import com.dashdive.internal.extraction.S3DistinctEventDataExtractor;
import com.dashdive.internal.extraction.S3RoundTripInterceptor;
import com.dashdive.internal.telemetry.TelemetryPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.GetBucketIntelligentTieringConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketIntelligentTieringConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class InternalExtractionTest {
  private static final String DATE_HEADER_VAL = "Wed, 01 Apr 2020 00:00:00 GMT";
  private static final String EXTRACTED_DATE_VAL = "2020-04-01T00:00:00Z";

  private static MockHttpClient sendSingleEvent(String testName, InterceptorContext context) {
    final MockHttpClient ignoredMockHttpClient = new MockHttpClient();
    final MockHttpClient batchMockHttpClient = new MockHttpClient();

    final int BATCH_SIZE = 1;
    final SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder()
                    .classInstanceId("test-extract-" + testName)
                    .build())
            .targetEventBatchSize(BATCH_SIZE)
            .startupTelemetryWarnings(TelemetryPayload.of())
            .build();

    final DashdiveImpl dashdive =
        new DashdiveImpl(
            Dashdive.DEFAULT_INGEST_BASE_URI,
            TestUtils.API_KEY_DUMMY,
            Optional.of(TestUtils.EXTRACTOR_CUSTOMER),
            Optional.empty(),
            ignoredMockHttpClient.getDelegate(),
            ignoredMockHttpClient.getDelegate(),
            batchMockHttpClient.getDelegate(),
            ignoredMockHttpClient.getDelegate(),
            Optional.of(setupDefaults));

    final S3RoundTripInterceptor interceptor = dashdive.getInterceptorForImperativeTrigger();
    interceptor.afterExecution(context, TestUtils.EXEC_ATTRS_EMPTY);

    dashdive.close();
    dashdive.blockUntilShutdownComplete();

    return batchMockHttpClient;
  }

  private static void doSingleEventExtractionTest(
      String testName,
      SdkRequest pojoRequest,
      SdkResponse pojoResponse,
      SdkHttpRequest httpRequest,
      SdkHttpResponse httpResponse,
      Map<String, Object> expectedPayload) {
    doSingleEventExtractionTest(
        testName, pojoRequest, pojoResponse, httpRequest, httpResponse, expectedPayload, false);
  }

  private static void doSingleEventExtractionTest(
      String testName,
      SdkRequest pojoRequest,
      SdkResponse pojoResponse,
      SdkHttpRequest httpRequest,
      SdkHttpResponse httpResponse,
      Map<String, Object> expectedPayload,
      boolean debug) {
    final InterceptorContext context =
        InterceptorContext.builder()
            .request(pojoRequest)
            .response(pojoResponse)
            .httpRequest(httpRequest)
            .httpResponse(httpResponse)
            .build();
    final MockHttpClient batchMockHttpClient = sendSingleEvent(testName, context);

    final List<String> batchIngestBodies =
        batchMockHttpClient.unboxRequestBodiesAssertingNonempty();
    batchMockHttpClient.assertAllUrisMatch(
        ConnectionUtils.getFullUri(
            Dashdive.DEFAULT_INGEST_BASE_URI, ConnectionUtils.Route.S3_BATCH_INGEST).getPath());
    final List<Map<String, Object>> ingestedEvents =
        TestUtils.getIngestedEventsFromRequestBodies(batchIngestBodies);

    Assertions.assertEquals(1, ingestedEvents.size());
    if (debug) {
      System.out.println("Difference: " + Maps.difference(expectedPayload, ingestedEvents.get(0)));
    }
    Assertions.assertEquals(expectedPayload, ingestedEvents.get(0));
  }

  // One test should have realistic req/res headers; we choose this one arbitrarily
  @Test
  void testGetObject() {
    final String OBJECT_KEY = "test-key";
    final String BUCKET_NAME = "test-bucket";
    final int OBJECT_SIZE = 1159893;

    final GetObjectRequest artificialPojoRequest =
        GetObjectRequest.builder().bucket(BUCKET_NAME).key(OBJECT_KEY).versionId(null).build();
    final GetObjectResponse artificialPojoResponse =
        GetObjectResponse.builder()
            .acceptRanges("bytes")
            .lastModified(Instant.now())
            .contentLength((long) OBJECT_SIZE)
            .eTag("657b8d98dc42ef950dabd894cbb827ea")
            .versionId(null)
            .contentType("application/octet-stream")
            .metadata(Map.of())
            .build();
    final SdkHttpRequest artificialSdkHttpRequest =
        SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.GET)
            .protocol("https")
            .host(BUCKET_NAME + ".s3.us-west-1.amazonaws.com")
            .encodedPath("/" + OBJECT_KEY)
            .headers(
                ImmutableMap.of(
                    "amz-sdk-invocation-id", List.of("dcba4308-cf8e-3be0-9267-bd404664f063"),
                    "amz-sdk-request", List.of("attempt=1; max=4"),
                    "Authorization", List.of("<some_auth>"),
                    "Signature", List.of("<some_signature>"),
                    "Host", List.of(BUCKET_NAME + ".s3.us-west-1.amazonaws.com"),
                    "User-Agent", List.of("aws-sdk-java/<version>"),
                    "x-amz-content-sha256", List.of("UNSIGNED-PAYLOAD"),
                    "X-Amz-Date", List.of("20240404T062548Z"),
                    "x-amz-te", List.of("append-md5")))
            .build();
    final SdkHttpResponse artificialSdkHttpResponse =
        SdkHttpResponse.builder()
            .statusCode(200)
            .statusText("OK")
            .headers(
                ImmutableMap.of(
                    "Accept-Ranges", List.of("bytes"),
                    "Content-Length", List.of(Integer.toString(OBJECT_SIZE)),
                    "Content-Type", List.of("application/octet-stream"),
                    "Date", List.of(DATE_HEADER_VAL),
                    "ETag", List.of("\"657b8d98dc42ef950dabd894cbb827ea\""),
                    "Last-Modified", List.of("<some_other_date>"),
                    "x-amz-id-2",
                        List.of(
                            "sLJWOdopXu4YGGbXFZJvtnHEKnrQs/Vg3uhiqtMl14QjM+xzbMel+lVmkui3BdZH/34dC58LGmA="),
                    "x-amz-request-id", List.of("PMBSH0W0J3TSHNHD"),
                    "x-amz-transfer-encoding", List.of("append-md5"),
                    "x-amz-version-id", List.of("null")))
            .build();

    final Map<String, Object> expectedPayload =
        Map.of(
            "action",
            "GetObject",
            "timestamp",
            EXTRACTED_DATE_VAL,
            "provider",
            "aws",
            "bucket",
            BUCKET_NAME,
            "objectKey",
            OBJECT_KEY,
            "versionId",
            "",
            "bytes",
            OBJECT_SIZE,
            "isBillableEgress",
            true,
            "customerId",
            TestUtils.EXTRACTOR_CUSTOMER_VAL);

    doSingleEventExtractionTest(
        "GetObject",
        artificialPojoRequest,
        artificialPojoResponse,
        artificialSdkHttpRequest,
        artificialSdkHttpResponse,
        expectedPayload);
  }

  @Test
  void testPutObject() {
    final String OBJECT_KEY = "test-key";
    final String BUCKET_NAME = "test-bucket";
    final String VERSION_ID = "test-version-id";
    final int OBJECT_SIZE = 4096;

    final PutObjectRequest artificialPojoRequest =
        PutObjectRequest.builder()
            .bucket(BUCKET_NAME)
            .key(OBJECT_KEY)
            .contentLength((long) OBJECT_SIZE)
            .build();
    final PutObjectResponse artificialPojoResponse =
        PutObjectResponse.builder().versionId(VERSION_ID).build();
    final SdkHttpRequest artificialSdkHttpRequest =
        SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.PUT)
            .protocol("https")
            .host(BUCKET_NAME + ".s3.us-west-1.amazonaws.com")
            .encodedPath("/" + OBJECT_KEY)
            .build();
    final SdkHttpResponse artificialSdkHttpResponse =
        SdkHttpResponse.builder()
            .statusCode(200)
            .statusText("OK")
            .headers(ImmutableMap.of("Date", List.of(DATE_HEADER_VAL)))
            .build();

    final Map<String, Object> expectedPayload =
        Map.of(
            "action",
            "PutObject",
            "timestamp",
            EXTRACTED_DATE_VAL,
            "provider",
            "aws",
            "bucket",
            BUCKET_NAME,
            "objectKey",
            OBJECT_KEY,
            "versionId",
            VERSION_ID,
            "bytes",
            OBJECT_SIZE,
            "customerId",
            TestUtils.EXTRACTOR_CUSTOMER_VAL);

    doSingleEventExtractionTest(
        "PutObject",
        artificialPojoRequest,
        artificialPojoResponse,
        artificialSdkHttpRequest,
        artificialSdkHttpResponse,
        expectedPayload);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testExtractionException() {
    final String OBJECT_KEY = "test-key";
    final String BUCKET_NAME = "test-bucket";
    final String VERSION_ID = "test-version-id";

    final PutObjectRequest artificialPojoRequest =
        // Intentionally omit the required field `contentLength` to trigger exception
        PutObjectRequest.builder().bucket(BUCKET_NAME).key(OBJECT_KEY).build();
    final PutObjectResponse artificialPojoResponse =
        PutObjectResponse.builder().versionId(VERSION_ID).build();
    final SdkHttpRequest artificialSdkHttpRequest =
        SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.PUT)
            .protocol("https")
            .host(BUCKET_NAME + ".s3.us-west-1.amazonaws.com")
            .encodedPath("/" + OBJECT_KEY)
            .build();
    final SdkHttpResponse artificialSdkHttpResponse =
        SdkHttpResponse.builder()
            .statusCode(200)
            .statusText("OK")
            .headers(ImmutableMap.of("Date", List.of(DATE_HEADER_VAL)))
            .build();

    final InterceptorContext context =
        InterceptorContext.builder()
            .request(artificialPojoRequest)
            .response(artificialPojoResponse)
            .httpRequest(artificialSdkHttpRequest)
            .httpResponse(artificialSdkHttpResponse)
            .build();
    final MockHttpClient batchMockHttpClient = sendSingleEvent("extraction_exception", context);

    batchMockHttpClient.assertAllUrisMatch(
        ConnectionUtils.getFullUri(
            Dashdive.DEFAULT_INGEST_BASE_URI, ConnectionUtils.Route.TELEMETRY_EXTRACTION_ISSUES)
            .getPath());
    final List<String> issueEventStrings =
        batchMockHttpClient.unboxRequestBodiesAssertingNonempty();

    final ObjectMapper objectMapper = new ObjectMapper();
    final TypeReference<Map<String, Object>> typeReference =
        new TypeReference<Map<String, Object>>() {};
    final List<Map<String, Object>> issueEventObjects =
        issueEventStrings.stream()
            .map(
                body -> {
                  try {
                    final Map<String, Object> singleEvent =
                        objectMapper.readValue(body, typeReference);
                    return singleEvent;
                  } catch (JsonProcessingException exception) {
                    Assertions.fail("Failed to parse JSON request body: " + exception.getMessage());
                    return null;
                  }
                })
            .collect(Collectors.toList());

    Assertions.assertEquals(1, issueEventObjects.size());
    final List<Map<String, Object>> eventsWithIssues =
        (List<Map<String, Object>>) issueEventObjects.get(0).get("eventsWithIssues");

    Assertions.assertEquals(1, eventsWithIssues.size());
    final Map<String, Object> eventWithIssues = eventsWithIssues.get(0);

    Assertions.assertEquals(true, eventWithIssues.get("hasIrrecoverableErrors"));
    Assertions.assertEquals(0, ((List<Object>) eventWithIssues.get("telemetryWarnings")).size());

    final List<Map<String, Object>> telemetryErrors =
        (List<Map<String, Object>>) eventWithIssues.get("telemetryErrors");
    Assertions.assertEquals(1, telemetryErrors.size());
    Assertions.assertEquals("uncaughtExtractionException", telemetryErrors.get(0).get("type"));
  }

  @Test
  void testUnknownEventType() {
    final String BUCKET_NAME = "test-bucket";

    final GetBucketIntelligentTieringConfigurationRequest artificialPojoRequest =
        GetBucketIntelligentTieringConfigurationRequest.builder().bucket(BUCKET_NAME).build();
    final GetBucketIntelligentTieringConfigurationResponse artificialPojoResponse =
        GetBucketIntelligentTieringConfigurationResponse.builder().build();
    final SdkHttpRequest artificialSdkHttpRequest =
        SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.POST)
            .protocol("https")
            .host(BUCKET_NAME + ".s3.us-west-1.amazonaws.com")
            .encodedPath("/?intelligent-tiering&id=Id")
            .build();
    final SdkHttpResponse artificialSdkHttpResponse =
        SdkHttpResponse.builder()
            .statusCode(200)
            .statusText("OK")
            .headers(ImmutableMap.of("Date", List.of(DATE_HEADER_VAL)))
            .build();

    final String reqClassName = artificialPojoRequest.getClass().getName();
    final String resClassName = artificialPojoResponse.getClass().getName();
    final String serializedClassNames =
        S3DistinctEventDataExtractor._serializeRoundTripClassNames(reqClassName, resClassName);

    final Map<String, Object> expectedPayload =
        Map.of(
            "action",
            "Unknown",
            "timestamp",
            EXTRACTED_DATE_VAL,
            "provider",
            "aws",
            "classNames",
            serializedClassNames,
            "customerId",
            TestUtils.EXTRACTOR_CUSTOMER_VAL);

    doSingleEventExtractionTest(
        "unknown_event_type",
        artificialPojoRequest,
        artificialPojoResponse,
        artificialSdkHttpRequest,
        artificialSdkHttpResponse,
        expectedPayload);
  }
}
