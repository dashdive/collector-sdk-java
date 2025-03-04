package com.dashdive;

import com.dashdive.internal.ImmutableDashdiveInstanceInfo;
import com.dashdive.internal.ImmutableSetupDefaults;
import com.dashdive.internal.SetupDefaults;
import com.dashdive.internal.extraction.S3RoundTripInterceptor;
import com.dashdive.internal.telemetry.TelemetryPayload;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class InterceptorIdempotencyTest {
  private static List<ExecutionInterceptor> withoutAwsInternalInterceptors(
      List<ExecutionInterceptor> interceptors) {
    final String awsInterceptorPrefix = "software.amazon.awssdk.";
    return interceptors.stream()
        .filter(interceptor -> !interceptor.getClass().getName().startsWith(awsInterceptorPrefix))
        .collect(Collectors.toList());
  }

  @Test
  void constructorsWorkIdempotently() {
    MockHttpClient sharedMockedClient = new MockHttpClient();

    SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder()
                    .classInstanceId("constructors-work-idempotently")
                    .build())
            .targetEventBatchSize(100)
            .startupTelemetryWarnings(TelemetryPayload.of())
            .build();
    DashdiveImpl dashdive =
        new DashdiveImpl(
            Dashdive.DEFAULT_INGEST_BASE_URI,
            TestUtils.API_KEY_DUMMY,
            Optional.of(TestUtils.EXTRACTOR_EMPTY),
            Optional.empty(),
            sharedMockedClient.getDelegate(),
            sharedMockedClient.getDelegate(),
            sharedMockedClient.getDelegate(),
            sharedMockedClient.getDelegate(),
            Optional.of(setupDefaults));

    ClientOverrideConfiguration.Builder combinedOverrideConfigurationBuilder =
        ClientOverrideConfiguration.builder().headers(Map.of("dummy-key", List.of("dummy-value")));
    combinedOverrideConfigurationBuilder =
        dashdive.addInterceptorTo(combinedOverrideConfigurationBuilder);
    combinedOverrideConfigurationBuilder =
        dashdive.addInterceptorTo(combinedOverrideConfigurationBuilder);
    S3ClientBuilder combinedS3ClientBuilder =
        S3Client.builder()
            .region(Region.US_WEST_1)
            .overrideConfiguration(combinedOverrideConfigurationBuilder.build());
    combinedS3ClientBuilder = dashdive.addConfigWithInterceptorTo(combinedS3ClientBuilder);
    combinedS3ClientBuilder = dashdive.addConfigWithInterceptorTo(combinedS3ClientBuilder);
    final S3Client combinedS3Client = combinedS3ClientBuilder.build();

    ClientOverrideConfiguration.Builder interceptorOverrideConfigurationBuilder =
        ClientOverrideConfiguration.builder();
    interceptorOverrideConfigurationBuilder =
        dashdive.addInterceptorTo(interceptorOverrideConfigurationBuilder);
    interceptorOverrideConfigurationBuilder =
        dashdive.addInterceptorTo(interceptorOverrideConfigurationBuilder);
    final S3Client interceptorS3Client =
        S3Client.builder()
            .region(Region.US_EAST_1)
            .overrideConfiguration(interceptorOverrideConfigurationBuilder.build())
            .build();

    S3ClientBuilder instrumentationS3ClientBuilder = S3Client.builder().region(Region.EU_CENTRAL_1);
    instrumentationS3ClientBuilder =
        dashdive.addConfigWithInterceptorTo(instrumentationS3ClientBuilder);
    instrumentationS3ClientBuilder =
        dashdive.addConfigWithInterceptorTo(instrumentationS3ClientBuilder);
    final S3Client instrumentationS3Client = instrumentationS3ClientBuilder.build();

    final List<ExecutionInterceptor> combinedList =
        withoutAwsInternalInterceptors(
            combinedS3Client
                .serviceClientConfiguration()
                .overrideConfiguration()
                .executionInterceptors());
    final List<ExecutionInterceptor> interceptorList =
        withoutAwsInternalInterceptors(
            interceptorS3Client
                .serviceClientConfiguration()
                .overrideConfiguration()
                .executionInterceptors());
    final List<ExecutionInterceptor> instrumentationList =
        withoutAwsInternalInterceptors(
            instrumentationS3Client
                .serviceClientConfiguration()
                .overrideConfiguration()
                .executionInterceptors());

    Assertions.assertEquals(1, combinedList.size());
    Assertions.assertInstanceOf(S3RoundTripInterceptor.class, combinedList.get(0));

    Assertions.assertEquals(1, interceptorList.size());
    Assertions.assertInstanceOf(S3RoundTripInterceptor.class, interceptorList.get(0));

    Assertions.assertEquals(1, instrumentationList.size());
    Assertions.assertInstanceOf(S3RoundTripInterceptor.class, instrumentationList.get(0));

    combinedS3Client.close();
    interceptorS3Client.close();
    instrumentationS3Client.close();
    dashdive.close();
  }
}
