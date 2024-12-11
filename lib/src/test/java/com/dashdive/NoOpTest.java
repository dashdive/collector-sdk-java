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

import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetBucketIntelligentTieringConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketIntelligentTieringConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class NoOpTest {
    private static List<ExecutionInterceptor> withoutAwsInternalInterceptors(
        List<ExecutionInterceptor> interceptors) {
        final String awsInterceptorPrefix = "software.amazon.awssdk.";
        return interceptors.stream()
            .filter(interceptor -> !interceptor.getClass().getName().startsWith(awsInterceptorPrefix))
            .collect(Collectors.toList());
    }

  @Test
  void testNoOpWhenRequiredArgMissing() {
    final Dashdive dashdive = Dashdive.builder().apiKey("my api key").build();

    ClientOverrideConfiguration.Builder combinedOverrideConfigurationBuilder =
    ClientOverrideConfiguration.builder().headers(Map.of("dummy-key", List.of("dummy-value")));
    combinedOverrideConfigurationBuilder =
        dashdive.addInterceptorTo(combinedOverrideConfigurationBuilder);
    S3ClientBuilder combinedS3ClientBuilder =
        S3Client.builder()
            .region(Region.US_WEST_1)
            .overrideConfiguration(combinedOverrideConfigurationBuilder.build());
    combinedS3ClientBuilder = dashdive.addConfigWithInterceptorTo(combinedS3ClientBuilder);
    final S3Client combinedS3Client = combinedS3ClientBuilder.build();

    final List<ExecutionInterceptor> combinedList =
        withoutAwsInternalInterceptors(
            combinedS3Client
                .serviceClientConfiguration()
                .overrideConfiguration()
                .executionInterceptors());

    Assertions.assertEquals(0, combinedList.size());

    combinedS3Client.close();
    dashdive.close();
  }
}
