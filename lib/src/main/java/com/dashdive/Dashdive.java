package com.dashdive;

import com.dashdive.internal.DashdiveConnection;
import com.dashdive.internal.DashdiveInstanceInfo;
import com.dashdive.internal.ImmutableDashdiveInstanceInfo;
import com.dashdive.internal.InitialSetupWorker;
import com.dashdive.internal.SetupDefaults;
import com.dashdive.internal.batching.BatchEventProcessor;
import com.dashdive.internal.batching.SingleEventBatcher;
import com.dashdive.internal.extraction.S3RoundTripInterceptor;
import com.dashdive.internal.telemetry.ImmutableTelemetryEvent;
import com.dashdive.internal.telemetry.TelemetryEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

class Base64UUID {
  private static Encoder encoder = Base64.getUrlEncoder().withoutPadding();

  public static String generate() {
    return encoder.encodeToString(asBytes(UUID.randomUUID()));
  }

  private static byte[] asBytes(UUID uuid) {
    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
    byteBuffer.putLong(uuid.getMostSignificantBits());
    byteBuffer.putLong(uuid.getLeastSignificantBits());
    return byteBuffer.array();
  }
}

@Value.Style(newBuilder = "builder")
public class Dashdive implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(Dashdive.class);

  public static final String VERSION = "1.0.0";
  private final String instanceId;

  private final String apiKey;
  private final S3RoundTripInterceptor s3RoundTripInterceptor;
  private final SingleEventBatcher singleEventBatcher;
  private final BatchEventProcessor batchEventProcessor;

  private final InitialSetupWorker initialSetupWorker;
  private final Thread initialSetupWorkerThread;

  private final AtomicBoolean isInitialized;
  private final AtomicReference<DashdiveInstanceInfo> instanceInfo;
  private final AtomicInteger targetEventBatchSize;

  private final HttpClient dashdiveHttpClient;

  // TODO: Maybe send list of execution interceptors to our telemetry endpoint.
  // Helps us understand any unexpected modifications pre- or post- Dashdive data collection:
  // "s3Client.serviceClientConfiguration().overrideConfiguration().executionInterceptors()
  //              .forEach(System.out::println);"

  // Constructors are package-private instead of fully private for testing,
  // to allow for mocked HTTP clients
  Dashdive(
      String apiKey,
      Optional<S3EventAttributeExtractorFactory> s3EventAttributeExtractorFactory,
      Optional<Duration> shutdownGracePeriod,
      HttpClient dashdiveHttpClient,
      HttpClient setupHttpClient,
      HttpClient batchProcessorHttpClient,
      HttpClient metricsHttpClient,
      Optional<SetupDefaults> skipSetupWithDefaults,
      boolean shouldSkipImdsQueries) {
    this.dashdiveHttpClient = dashdiveHttpClient;

    this.isInitialized = new AtomicBoolean(false);
    // Initialize with defaults in case async setup fails
    Optional<String> defaultClassInstanceId =
        skipSetupWithDefaults.flatMap(d -> d.dashdiveInstanceInfo().classInstanceId());
    this.instanceId = defaultClassInstanceId.orElse(Base64UUID.generate());
    this.instanceInfo =
        new AtomicReference<>(
            ImmutableDashdiveInstanceInfo.builder().classInstanceId(instanceId).build());
    this.targetEventBatchSize = new AtomicInteger(SingleEventBatcher.DEFAULT_TARGET_BATCH_SIZE);

    this.apiKey = apiKey;
    if (!s3EventAttributeExtractorFactory.isPresent()) {
      logger.warn(
          "No S3 event attribute extractor factory provided; using factory for no-op extractor.");
    }
    final S3EventAttributeExtractorFactory presentS3EventAttributeExtractorFactory =
        s3EventAttributeExtractorFactory.orElse(() -> new NoOpS3EventAttributeExtractor());
    this.batchEventProcessor =
        new BatchEventProcessor(
            this.instanceInfo,
            apiKey,
            presentS3EventAttributeExtractorFactory,
            shutdownGracePeriod,
            batchProcessorHttpClient,
            metricsHttpClient);
    this.singleEventBatcher =
        new SingleEventBatcher(isInitialized, targetEventBatchSize, batchEventProcessor);
    this.s3RoundTripInterceptor = new S3RoundTripInterceptor(this.singleEventBatcher);

    this.initialSetupWorker =
        new InitialSetupWorker(
            setupHttpClient,
            apiKey,
            skipSetupWithDefaults,
            shouldSkipImdsQueries,
            isInitialized,
            instanceInfo,
            targetEventBatchSize,
            Optional.of(() -> this.batchEventProcessor.notifyInitialized()));
    this.initialSetupWorkerThread = new Thread(this.initialSetupWorker);
    this.initialSetupWorkerThread.start();

    logger.info("Dashdive instance created with id: {}", instanceId);
  }

  Dashdive(
      String apiKey,
      Optional<S3EventAttributeExtractorFactory> s3EventAttributeExtractorFactory,
      Optional<Duration> shutdownGracePeriod,
      HttpClient dashdiveHttpClient,
      HttpClient setupHttpClient,
      HttpClient batchProcessorHttpClient,
      HttpClient metricsHttpClient,
      Optional<SetupDefaults> skipSetupWithDefaultss) {
    this(
        apiKey,
        s3EventAttributeExtractorFactory,
        shutdownGracePeriod,
        dashdiveHttpClient,
        setupHttpClient,
        batchProcessorHttpClient,
        metricsHttpClient,
        skipSetupWithDefaultss,
        false);
  }

  Dashdive(
      String apiKey,
      Optional<S3EventAttributeExtractorFactory> s3EventAttributeExtractorFactory,
      Optional<Duration> shutdownGracePeriod) {
    this(Optional.of(apiKey), s3EventAttributeExtractorFactory, shutdownGracePeriod);
  }

  @Builder.Constructor
  Dashdive(
      Optional<String> apiKey,
      Optional<S3EventAttributeExtractorFactory> s3EventAttributeExtractorFactory,
      Optional<Duration> shutdownGracePeriod) {
    // No need to check the user-supplied values for null, since the Immutables
    // builder automatically enforces non-null
    this(
        apiKey.orElse(""),
        s3EventAttributeExtractorFactory,
        shutdownGracePeriod,
        DashdiveConnection.directExecutorHttpClient(),
        DashdiveConnection.directExecutorHttpClient(),
        DashdiveConnection.directExecutorHttpClient(),
        DashdiveConnection.directExecutorHttpClient(),
        Optional.empty(),
        false);
  }

  public static DashdiveBuilder builder() {
    return DashdiveBuilder.builder();
  }

  // Requiring an explicit `close` call is OK in certain circumstances and has
  // precedence with other robust SDKs, such as the AWS SDK:
  // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/imds/Ec2MetadataClient.html#closing-the-client-heading
  //
  // Additionally, `close` is auto-inferred by Spring as the default destroy method
  // (auto-called at end of program). See: https://stackoverflow.com/a/44757112/14816795
  public void close() {
    try {
      closeUnsafe();
    } catch (Exception exception) {
      logger.error("Exception while closing Dashdive instance", exception);
    }
  }

  private void closeUnsafe() {
    try {
      this.initialSetupWorkerThread.join();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }

    // Important that the batcher is flushed before the processor
    // so we don't lose any events (all batches are sent to processor)
    this.singleEventBatcher.shutDownAndFlush();
    this.batchEventProcessor.shutDownAndFlush();

    final ImmutableMap<String, Integer> metricsSinceInception =
        this.batchEventProcessor.getSerializableMetricsSinceInception();
    logger.info("Dashdive instance shutting down. Lifetime metrics: {}", metricsSinceInception);

    try {
      final ObjectMapper objectMapper = DashdiveConnection.DEFAULT_SERIALIZER;
      final TelemetryEvent.LifecycleShutdown shutdownPayload =
          ImmutableTelemetryEvent.LifecycleShutdown.builder()
              .instanceId(instanceId)
              .metricsTotal(metricsSinceInception)
              .build();
      final String requestBodyJson = objectMapper.writeValueAsString(shutdownPayload);
      final HttpRequest shutdownTelemetryRequest =
          HttpRequest.newBuilder()
              .uri(DashdiveConnection.getRoute(DashdiveConnection.Route.TELEMETRY_LIFECYCLE))
              .header(
                  DashdiveConnection.Headers.KEY__CONTENT_TYPE,
                  DashdiveConnection.Headers.VAL__CONTENT_JSON)
              .header(
                  DashdiveConnection.Headers.KEY__USER_AGENT,
                  DashdiveConnection.Headers.getUserAgent(
                      instanceInfo.get().javaVersion(),
                      Optional.of(VERSION),
                      Optional.of(instanceId)))
              .header(DashdiveConnection.Headers.KEY__API_KEY, apiKey)
              .POST(HttpRequest.BodyPublishers.ofString(requestBodyJson))
              .build();
      DashdiveConnection.send(dashdiveHttpClient, shutdownTelemetryRequest);
    } catch (IOException | InterruptedException ignored) {
    }
  }

  private ClientOverrideConfiguration.Builder addInterceptorIdempotentlyTo(
      final ClientOverrideConfiguration.Builder overrideConfigBuilder) {
    final boolean alreadyHasDashdiveInterceptor =
        overrideConfigBuilder.executionInterceptors().stream()
            .anyMatch(interceptor -> interceptor instanceof S3RoundTripInterceptor);
    return alreadyHasDashdiveInterceptor
        ? overrideConfigBuilder
        : overrideConfigBuilder.addExecutionInterceptor(this.s3RoundTripInterceptor);
  }

  public ClientOverrideConfiguration.Builder withInterceptor(
      final ClientOverrideConfiguration.Builder overrideConfigBuilder) {
    return addInterceptorIdempotentlyTo(overrideConfigBuilder);
  }

  // Once this method is called on the `clientBuilder`, the caller should not
  // call `clientBuilder.overrideConfiguration(...)` again, or the interceptor will be erased.
  public S3ClientBuilder withNewOverrideConfigHavingInstrumentation(
      final S3ClientBuilder clientBuilder) {
    final ClientOverrideConfiguration newOverrideConfig =
        addInterceptorIdempotentlyTo(ClientOverrideConfiguration.builder()).build();
    return clientBuilder.overrideConfiguration(newOverrideConfig);
  }

  @VisibleForTesting
  S3RoundTripInterceptor getInterceptorForImperativeTrigger() {
    return this.s3RoundTripInterceptor;
  }

  @VisibleForTesting
  void blockUntilSetupComplete() {
    try {
      this.initialSetupWorkerThread.join();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  void blockUntilShutdownComplete() {
    try {
      this.initialSetupWorkerThread.join();
      this.singleEventBatcher._blockUntilShutdownComplete();
      this.batchEventProcessor._blockUntilShutdownComplete();
    } catch (InterruptedException ignored) {
    }
  }
}
