package com.dashdive;

import com.dashdive.internal.extraction.S3RoundTripExtractor;
import com.dashdive.internal.telemetry.EventPipelineMetrics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.SSLSession;
import org.immutables.value.Value;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

class TestUtils {
  public static final String API_KEY_DUMMY =
      "WOQOMXLP.wtM1TehdxPx1P27dhcJqrGoRETCtMCxLuhcAmte1PjD2lq1A";
  public static final String API_KEY_STAGING =
      "DKQJNJVH.u9Itt0g2mm+FTrxyxm0rh1PvL4hOyzQLKV2avZs3MCZHr2FF";
  public static final String API_KEY_PRODUCTION =
      "IIOQRHEC.Dzi3Au0n0NSq5VBcB2e0KgovA/EATJt4vHzdBFNusybUIgOw";

  public static final S3EventAttributeExtractorFactory FACTORY_EMPTY =
      () -> (e) -> ImmutableS3EventAttributes.of();
  public static final String FACTORY_CUSTOMER_VAL = "dummy-customer";
  public static final S3EventAttributeExtractorFactory FACTORY_CUSTOMER =
      () -> (e) -> ImmutableS3EventAttributes.builder().customerId(FACTORY_CUSTOMER_VAL).build();

  public static final ExecutionAttributes EXEC_ATTRS_EMPTY = ExecutionAttributes.builder().build();

  public static final InterceptorContext GENERIC_INTERCEPTED_EVENT =
      getListObjectsV2Event("test-bucket");

  // Used as a makeshift "simplest possible" event with the bucket name as an ID of sorts
  public static InterceptorContext getListObjectsV2Event(String bucketName) {
    final ListObjectsV2Request artificialPojoRequest =
        ListObjectsV2Request.builder().bucket(bucketName).build();
    final ListObjectsV2Response artificialPojoResponse =
        ListObjectsV2Response.builder().contents(List.of()).build();
    final SdkHttpRequest artificialSdkHttpRequest =
        SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.GET)
            .protocol("https")
            .host(bucketName + ".s3.us-west-1.amazonaws.com")
            .build();
    final SdkHttpResponse artificialSdkHttpResponse =
        SdkHttpResponse.builder()
            .putHeader(
                S3RoundTripExtractor.S3_RESPONSE_DATE_HEADER,
                DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now()))
            .statusCode(200)
            .statusText("OK")
            .build();

    return InterceptorContext.builder()
        .request(artificialPojoRequest)
        .response(artificialPojoResponse)
        .httpRequest(artificialSdkHttpRequest)
        .httpResponse(artificialSdkHttpResponse)
        .build();
  }

  public static List<Map<String, Object>> getObjectsFromRequestBodies(List<String> requestBodies) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final TypeReference<Map<String, Object>> typeReference =
        new TypeReference<Map<String, Object>>() {};
    final List<Map<String, Object>> requestObjects =
        requestBodies.stream()
            .map(
                body -> {
                  try {
                    final Map<String, Object> eventsBatch =
                        objectMapper.readValue(body, typeReference);
                    return eventsBatch;
                  } catch (JsonProcessingException exception) {
                    Assertions.fail("Failed to parse JSON request body: " + exception.getMessage());
                    return null;
                  }
                })
            .collect(Collectors.toList());
    return requestObjects;
  }

  public static List<Map<String, Object>> getIngestedEventsFromRequestBodies(
      List<String> batchIngestBodies) {
    final ObjectMapper objectMapper = new ObjectMapper();
    final TypeReference<List<Map<String, Object>>> typeReference =
        new TypeReference<List<Map<String, Object>>>() {};
    final List<Map<String, Object>> events =
        batchIngestBodies.stream()
            .map(
                body -> {
                  try {
                    final List<Map<String, Object>> eventsBatch =
                        objectMapper.readValue(body, typeReference);
                    return eventsBatch;
                  } catch (JsonProcessingException exception) {
                    Assertions.fail("Failed to parse JSON request body: " + exception.getMessage());
                    return null;
                  }
                })
            .flatMap(List::stream)
            .collect(Collectors.toList());
    return events;
  }
}

class MetricsUtils {
  public static enum Type {
    TOTAL,
    INCREMENTAL
  }

  private static class MetricsRequestBody {
    public static final String KEY_INSTANCE_ID = "instanceId";
    public static final String KEY_METRICS_TOTAL = "metricsTotal";
    public static final String KEY_METRICS_INCREMENTAL = "metricsIncremental";
    public static final String KEY_EVENT_TYPE = "eventType";

    public static final String EVENT_TYPE_TOTAL = "shutdown";
    public static final String EVENT_TYPE_INCREMENTAL = "metrics_incremental";
  }

  private static final Set<String> METRIC_TYPES =
      Stream.of(EventPipelineMetrics.Type.values())
          .map(EventPipelineMetrics.Type::toWireString)
          .collect(Collectors.toSet());

  public static Map<String, Integer> getMetricsFromRequestBodyWithAssertions(
      Type metricsRequestType, String requestBody, String expectedInstanceId) {
    final String metricsEventType =
        metricsRequestType == Type.INCREMENTAL
            ? MetricsRequestBody.EVENT_TYPE_INCREMENTAL
            : MetricsRequestBody.EVENT_TYPE_TOTAL;
    final String metricsKey =
        metricsRequestType == Type.INCREMENTAL
            ? MetricsRequestBody.KEY_METRICS_INCREMENTAL
            : MetricsRequestBody.KEY_METRICS_TOTAL;
    final Set<String> metricsRequestBodyKeys =
        Set.of(MetricsRequestBody.KEY_INSTANCE_ID, MetricsRequestBody.KEY_EVENT_TYPE, metricsKey);

    final ObjectMapper objectMapper = new ObjectMapper();
    final TypeReference<Map<String, Object>> typeRef = new TypeReference<Map<String, Object>>() {};
    Map<String, Object> parsedRequestBody = null;
    try {
      parsedRequestBody = objectMapper.readValue(requestBody, typeRef);
    } catch (JsonProcessingException exception) {
      Assertions.fail("Failed to parse JSON request body: " + exception.getMessage());
    }

    Assertions.assertEquals(metricsRequestBodyKeys, parsedRequestBody.keySet());
    Assertions.assertEquals(
        expectedInstanceId, parsedRequestBody.get(MetricsRequestBody.KEY_INSTANCE_ID));
    Assertions.assertEquals(
        metricsEventType, parsedRequestBody.get(MetricsRequestBody.KEY_EVENT_TYPE));

    Assertions.assertInstanceOf(Map.class, parsedRequestBody.get(metricsKey));
    @SuppressWarnings("unchecked")
    final Map<String, Integer> metricsData =
        (Map<String, Integer>) parsedRequestBody.get(metricsKey);
    Assertions.assertEquals(METRIC_TYPES, metricsData.keySet());

    return metricsData;
  }

  public static Map<String, Integer> getCombinedMetricsFromIncremental(
      List<Map<String, Integer>> incrementalMetrics) {
    return incrementalMetrics.stream()
        .flatMap(map -> map.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::sum));
  }
}

class MockHttpClient {
  // An HttpResopnse can't be created directly, so we need to instantiate our own mock. See:
  // https://github.com/openjdk/jdk21u-dev/blob/jdk-21%2B1/src/java.net.http/share/classes/jdk/internal/net/http/MultiExchange.java#L332
  // https://github.com/openjdk/jdk21u-dev/blob/jdk-21%2B1/src/java.net.http/share/classes/jdk/internal/net/http/HttpResponseImpl.java
  @Value.Immutable
  public abstract static class MockHttpResponse implements HttpResponse<String> {
    public abstract int statusCode();

    @Value.Default
    public HttpHeaders headers() {
      return HttpHeaders.of(ImmutableMap.of(), (k, v) -> true);
    }

    @Value.Default
    public String body() {
      return "";
    }

    public abstract HttpRequest request();

    public abstract URI uri();

    @Value.Default
    public HttpClient.Version version() {
      return HttpClient.Version.HTTP_1_1;
    }

    public abstract Optional<HttpResponse<String>> previousResponse();

    public abstract Optional<SSLSession> sslSession();
  }

  @Value.Immutable
  public abstract static class HttpRequestWithBody {
    public abstract HttpRequest request();

    public abstract Optional<String> body();
  }

  private final HttpClient mockedDelegate;
  private final ConcurrentLinkedQueue<HttpRequestWithBody> requests;

  private static class FullBodySubscriber implements Flow.Subscriber<ByteBuffer> {
    private int offset = 0;
    private String fullBody;
    private byte[] bodyBytes;

    public FullBodySubscriber(int length) {
      bodyBytes = new byte[length];
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
      subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(ByteBuffer item) {
      int remaining = bodyBytes.length - offset;
      int bytesToCopy = Math.min(item.remaining(), remaining);
      item.get(bodyBytes, offset, bytesToCopy);
      offset += bytesToCopy;
    }

    @Override
    public void onError(Throwable ignored) {}

    @Override
    public void onComplete() {
      fullBody = new String(bodyBytes, StandardCharsets.UTF_8);
    }

    public String getFullBody() {
      return fullBody;
    }
  }

  private static Optional<String> getBodyStringFrom(HttpRequest request) {
    if (request.bodyPublisher().isEmpty()) {
      return Optional.empty();
    }
    BodyPublisher bodyPublisher = request.bodyPublisher().get();
    long contentLength = bodyPublisher.contentLength();
    FullBodySubscriber subscriber = new FullBodySubscriber(Math.toIntExact(contentLength));
    bodyPublisher.subscribe(subscriber);
    return Optional.of(subscriber.getFullBody());
  }

  public MockHttpClient() {
    this(Optional.empty());
  }

  public MockHttpClient(Runnable requestAction) {
    this(Optional.of(requestAction));
  }

  @SuppressWarnings("unchecked")
  public MockHttpClient(Optional<Runnable> requestAction) {
    this.mockedDelegate = Mockito.mock(HttpClient.class);
    this.requests = new ConcurrentLinkedQueue<>();

    try {
      Mockito.when(
              mockedDelegate.send(
                  Mockito.any(HttpRequest.class), Mockito.any(HttpResponse.BodyHandler.class)))
          .thenAnswer(
              invocation -> {
                HttpRequest request = invocation.getArgument(0);
                Optional<String> requestBody = getBodyStringFrom(request);
                requests.add(
                    ImmutableHttpRequestWithBody.builder()
                        .request(request)
                        .body(requestBody)
                        .build());

                requestAction.ifPresent(Runnable::run);
                return ImmutableMockHttpResponse.builder()
                    .request(request)
                    .uri(request.uri())
                    .statusCode(200)
                    .build();
              });
    } catch (IOException | InterruptedException ignored) {
    }
  }

  public HttpClient getDelegate() {
    return mockedDelegate;
  }

  public List<HttpRequestWithBody> getRequests() {
    return List.of(requests.toArray(HttpRequestWithBody[]::new));
  }

  public void assertAllUrisMatch(String path) {
    Assertions.assertTrue(
        requests.stream().allMatch(req -> req.request().uri().getPath().equals(path)));
  }

  public static List<String> unboxRequestBodiesAssertingNonempty(
      List<HttpRequestWithBody> requests) {
    Assertions.assertTrue(requests.stream().allMatch(req -> req.body().isPresent()));
    return requests.stream()
        .map(HttpRequestWithBody::body)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  public List<String> unboxRequestBodiesAssertingNonempty() {
    return MockHttpClient.unboxRequestBodiesAssertingNonempty(getRequests());
  }
}
