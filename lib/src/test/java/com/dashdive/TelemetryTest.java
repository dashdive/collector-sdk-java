package com.dashdive;

import com.dashdive.internal.ImmutableDashdiveInstanceInfo;
import com.dashdive.internal.ImmutableSetupDefaults;
import com.dashdive.internal.SetupDefaults;
import com.dashdive.internal.extraction.S3RoundTripInterceptor;
import com.dashdive.internal.telemetry.ImmutableTelemetryItem;
import com.dashdive.internal.telemetry.TelemetryPayload;
import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class TelemetryTest {
  @Test
  void testAllTelemetryDisabled() {
    final String OBJECT_KEY = "test-key";
    final String BUCKET_NAME = "test-bucket";
    final String VERSION_ID = "test-version-id";
    final String DATE_HEADER_VAL = "Wed, 01 Apr 2020 00:00:00 GMT";

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

    final int BATCH_SIZE = 1;
    final SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder()
                    .classInstanceId("test-extract-extraction_exception")
                    .build())
            .targetEventBatchSize(BATCH_SIZE)
            .startupTelemetryWarnings(
                TelemetryPayload.from(
                    ImmutableTelemetryItem.builder()
                        .type("NETWORK_ERROR")
                        .data(ImmutableMap.of())
                        .build()))
            .build();

    final MockHttpClient mockHttpClient = new MockHttpClient();
    final Supplier<Boolean> disableAllTelemetry = () -> true;
    final DashdiveImpl dashdive =
        new DashdiveImpl(
            Dashdive.DEFAULT_INGEST_BASE_URI,
            TestUtils.API_KEY_DUMMY,
            Optional.of(TestUtils.EXTRACTOR_CUSTOMER),
            Optional.empty(),
            Optional.empty(),
            Optional.of(disableAllTelemetry),
            Optional.empty(),
            Optional.empty(),
            mockHttpClient.getDelegate(),
            mockHttpClient.getDelegate(),
            mockHttpClient.getDelegate(),
            mockHttpClient.getDelegate(),
            Optional.of(setupDefaults),
            false,
            DashdiveImpl.SERVICE_ID_TEST_SYSTEM_PROPERTY_KEY,
            Optional.of(Duration.ofMillis(0)));

    final S3RoundTripInterceptor interceptor = dashdive.getInterceptorForImperativeTrigger();
    interceptor.afterExecution(context, TestUtils.EXEC_ATTRS_EMPTY);

    dashdive.close();
    dashdive.blockUntilShutdownComplete();

    // Manually inspected without disabling supplier; confirmed that all telemetry event
    // types would show up here. We expect none since we're disabling globally.
    Assertions.assertEquals(0, mockHttpClient.getRequests().size());
  }
}
