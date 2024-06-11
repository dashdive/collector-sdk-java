package com.dashdive.internal;

import com.dashdive.Dashdive;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

public class DashdiveConnection {
  public static final URI INGEST_BASE_URI = URI.create("https://staging.ingest.dashdive.com");

  public static final ObjectMapper DEFAULT_SERIALIZER =
      new ObjectMapper().registerModule(new Jdk8Module());

  public static class Headers {
    public static String getUserAgent(
        Optional<String> javaVersion,
        Optional<String> dashdiveSdkVersion,
        Optional<String> classInstanceId) {
      // Taken from logic in HttpClientImpl.java which computes default user agent
      // https://github.com/openjdk/jdk21u-dev/blob/master/src/java.net.http/share/classes/jdk/internal/net/http/HttpRequestImpl.java#L69
      final String defaultUserAgent =
          "Java-http-client" + javaVersion.map((v) -> "/").orElse("") + javaVersion.orElse("");

      final Optional<String> prefixedDashdiveSdkVersion = dashdiveSdkVersion.map(str -> "v:" + str);
      final Optional<String> prefixedClassInstanceId = classInstanceId.map(str -> "id:" + str);
      return Stream.of(
              defaultUserAgent,
              prefixedDashdiveSdkVersion.orElse(""),
              prefixedClassInstanceId.orElse(""))
          .filter(s -> !s.isEmpty())
          .collect(Collectors.joining(" "));
    }

    public static String getUserAgentFromInstanceInfo(DashdiveInstanceInfo instanceInfo) {
      return getUserAgent(
          instanceInfo.javaVersion(),
          Optional.ofNullable(Dashdive.VERSION),
          instanceInfo.classInstanceId());
    }

    public static final String API_KEY = "X-API-Key";
    public static final String USER_AGENT = "User-Agent";
  }

  public static class Routes {
    public static final URI PING = INGEST_BASE_URI.resolve("/ping");

    public static final URI S3_RECOMMENDED_BATCH_SIZE =
        INGEST_BASE_URI.resolve("/s3/recommendedBatchSize");
    public static final URI S3_BATCH_INGEST = INGEST_BASE_URI.resolve("/s3/batch");

    public static final URI TELEMETRY_API_KEY = INGEST_BASE_URI.resolve("/telemetry/invalidApiKey");
    public static final URI TELEMETRY_LIFECYCLE = INGEST_BASE_URI.resolve("/telemetry/lifecycle");
    public static final URI TELEMETRY_EXTRACTION_ISSUES =
        INGEST_BASE_URI.resolve("/telemetry/extractionIssues");
    public static final URI TELEMETRY_METRICS =
        INGEST_BASE_URI.resolve("/telemetry/metricsIncremental");
  }

  public static class APIKey {
    private static final int PREFIX_LENGTH_ALPHANUM = 8;
    private static final int VALUE_LENGTH_BASE64 = 48;
    private static final Pattern REGEX_FULL =
        Pattern.compile(
            "^([A-Za-z0-9]{"
                + PREFIX_LENGTH_ALPHANUM
                + "}(?:\\.))?([A-Za-z0-9\\+\\/]{"
                + VALUE_LENGTH_BASE64
                + "})$");

    public static boolean isValid(String apiKey) {
      return REGEX_FULL.matcher(apiKey).matches();
    }
  }

  @Value.Immutable(singleton = true)
  public abstract static class BackoffSendConfig {
    @Value.Default
    public Duration initialBackoff() {
      return Duration.ofMillis(100);
    }

    @Value.Default
    public int maxTries() {
      return 3;
    }

    @Value.Default
    public int backoffMultiplierNumerator() {
      return 2;
    }

    @Value.Default
    public int backoffMultiplierDenominator() {
      return 1;
    }
  }

  public static final BackoffSendConfig DEFAULT_BACKOFF_CONFIG = ImmutableBackoffSendConfig.of();

  public static HttpResponse<String> sendWithExponentialBackoff(
      HttpClient httpClient, HttpRequest request, BackoffSendConfig config)
      throws InterruptedException, IOException {
    Duration backoff = config.initialBackoff();
    IOException latestException = new IOException();
    for (int tries = 0; tries < config.maxTries(); tries++) {
      try {
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      } catch (IOException exception) {
        latestException = exception;
      }

      Thread.sleep(backoff.toMillis());

      backoff =
          backoff
              .multipliedBy(config.backoffMultiplierNumerator())
              .dividedBy(config.backoffMultiplierDenominator());
    }

    throw latestException;
  }

  public static HttpResponse<String> send(HttpClient httpClient, HttpRequest request)
      throws InterruptedException, IOException {
    return sendWithExponentialBackoff(httpClient, request, DEFAULT_BACKOFF_CONFIG);
  }

  public static HttpClient directExecutorHttpClient() {
    return HttpClient.newBuilder().executor(MoreExecutors.directExecutor()).build();
  }
}
