package com.dashdive;

import com.dashdive.internal.ConnectionUtils;
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
import java.net.URI;
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
import java.util.function.Supplier;

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

class DashdiveImpl implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(DashdiveImpl.class);

  private final String instanceId;

  private final URI ingestBaseUri;
  private final String apiKey;
  private final S3RoundTripInterceptor s3RoundTripInterceptor;
  private final SingleEventBatcher singleEventBatcher;
  private final BatchEventProcessor batchEventProcessor;

  private final InitialSetupWorker initialSetupWorker;
  private final Thread initialSetupWorkerThread;

  private final AtomicBoolean isInitialized;
  private final AtomicBoolean isShutDown;
  private final AtomicReference<DashdiveInstanceInfo> instanceInfo;
  private final AtomicInteger targetEventBatchSize;

  private final HttpClient dashdiveHttpClient;

  // TODO: Maybe send list of execution interceptors to our telemetry endpoint.
  // Helps us understand any unexpected modifications pre- or post- Dashdive data
  // collection:
  // "s3Client.serviceClientConfiguration().overrideConfiguration().executionInterceptors()
  // .forEach(System.out::println);"

  // Constructors are package-private instead of fully private for testing,
  // to allow for mocked HTTP clients
  DashdiveImpl(
      URI ingestBaseUri,
      String apiKey,
      Optional<S3EventAttributeExtractor> s3EventAttributeExtractor,
      Optional<Duration> shutdownGracePeriod,
      Optional<Supplier<Boolean>> eventInclusionSampler,
      HttpClient dashdiveHttpClient,
      HttpClient setupHttpClient,
      HttpClient batchProcessorHttpClient,
      HttpClient metricsHttpClient,
      Optional<SetupDefaults> skipSetupWithDefaults,
      boolean shouldSkipImdsQueries) {
    this.dashdiveHttpClient = dashdiveHttpClient;

    this.isInitialized = new AtomicBoolean(false);
    // Initialize with defaults in case async setup fails
    Optional<String> defaultClassInstanceId = skipSetupWithDefaults
        .flatMap(d -> d.dashdiveInstanceInfo().classInstanceId());
    this.instanceId = defaultClassInstanceId.orElse(Base64UUID.generate());
    this.instanceInfo = new AtomicReference<>(
        ImmutableDashdiveInstanceInfo.builder().classInstanceId(instanceId).build());
    this.targetEventBatchSize = new AtomicInteger(SingleEventBatcher.DEFAULT_TARGET_BATCH_SIZE);

    this.ingestBaseUri = ingestBaseUri;
    this.apiKey = apiKey;
    if (!s3EventAttributeExtractor.isPresent()) {
      logger.warn(
          "No S3 event attribute extractor factory provided; using factory for no-op extractor.");
    }
    final S3EventAttributeExtractor presentS3EventAttributeExtractor = s3EventAttributeExtractor
        .orElse(new NoOpS3EventAttributeExtractor());
    this.batchEventProcessor = new BatchEventProcessor(
        this.instanceInfo,
        apiKey,
        ingestBaseUri,
        presentS3EventAttributeExtractor,
        shutdownGracePeriod,
        batchProcessorHttpClient,
        metricsHttpClient);
    this.singleEventBatcher = new SingleEventBatcher(
        isInitialized, targetEventBatchSize, batchEventProcessor, eventInclusionSampler);
    this.s3RoundTripInterceptor = new S3RoundTripInterceptor(this.singleEventBatcher);

    this.initialSetupWorker = new InitialSetupWorker(
        setupHttpClient,
        apiKey,
        ingestBaseUri,
        skipSetupWithDefaults,
        shouldSkipImdsQueries,
        isInitialized,
        instanceInfo,
        targetEventBatchSize,
        Optional.of(() -> this.batchEventProcessor.notifyInitialized()));
    this.initialSetupWorkerThread = new Thread(this.initialSetupWorker);
    this.initialSetupWorkerThread.setPriority(Thread.MIN_PRIORITY);
    this.initialSetupWorkerThread.start();
    this.isShutDown = new AtomicBoolean(false);

    logger.info("Dashdive instance created with id: {}", instanceId);
  }

  DashdiveImpl(
      URI ingestionBaseUri,
      String apiKey,
      Optional<S3EventAttributeExtractor> s3EventAttributeExtractor,
      Optional<Duration> shutdownGracePeriod,
      HttpClient dashdiveHttpClient,
      HttpClient setupHttpClient,
      HttpClient batchProcessorHttpClient,
      HttpClient metricsHttpClient,
      Optional<SetupDefaults> skipSetupWithDefaultss) {
    this(
        ingestionBaseUri,
        apiKey,
        s3EventAttributeExtractor,
        shutdownGracePeriod,
        Optional.empty(),
        dashdiveHttpClient,
        setupHttpClient,
        batchProcessorHttpClient,
        metricsHttpClient,
        skipSetupWithDefaultss,
        false);
  }

  DashdiveImpl(
      URI ingestionBaseUri,
      String apiKey,
      Optional<S3EventAttributeExtractor> s3EventAttributeExtractor,
      Optional<Duration> shutdownGracePeriod) {
    this(Optional.of(ingestionBaseUri), Optional.of(apiKey),
        s3EventAttributeExtractor, shutdownGracePeriod, Optional.empty());
  }

  DashdiveImpl(
      // All fields are optional to avoid NullPointerExceptions at runtime;
      // much better to have sends simply fail, for example, when apiKey is not
      // specified
      Optional<URI> ingestionBaseUri,
      Optional<String> apiKey,
      Optional<S3EventAttributeExtractor> s3EventAttributeExtractor,
      Optional<Duration> shutdownGracePeriod,
      Optional<Supplier<Boolean>> eventInclusionSampler) {
    // No need to check the user-supplied values for null, since the Immutables
    // builder automatically enforces non-null
    this(
        ingestionBaseUri.orElse(Dashdive.DEFAULT_INGEST_BASE_URI),
        apiKey.orElse(""),
        s3EventAttributeExtractor,
        shutdownGracePeriod,
        eventInclusionSampler,
        ConnectionUtils.directExecutorHttpClient(),
        ConnectionUtils.directExecutorHttpClient(),
        ConnectionUtils.directExecutorHttpClient(),
        ConnectionUtils.directExecutorHttpClient(),
        Optional.empty(),
        false);
  }

  public void close() {
    // Requiring an explicit `close` call is OK in certain circumstances and has
    // precedence with other robust SDKs, such as the AWS SDK:
    // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/imds/Ec2MetadataClient.html#closing-the-client-heading
    //
    // Additionally, `close` is auto-inferred by Spring as the default destroy
    // method
    // (auto-called at end of program). See:
    // https://stackoverflow.com/a/44757112/14816795
    try {
      closeUnsafe();
    } catch (Exception exception) {
      logger.error("Exception while closing Dashdive instance", exception);
    }
  }

  private void closeUnsafe() {
    if (this.isShutDown.getAndSet(true)) {
      return;
    }

    try {
      this.initialSetupWorkerThread.join();
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }

    // Important that the batcher is flushed before the processor
    // so we don't lose any events (all batches are sent to processor)
    this.singleEventBatcher.shutDownAndFlush();
    this.batchEventProcessor.shutDownAndFlush();

    final ImmutableMap<String, Integer> metricsSinceInception = this.batchEventProcessor
        .getSerializableMetricsSinceInception();
    logger.info("Dashdive instance shutting down. Lifetime metrics: {}", metricsSinceInception);

    try {
      final ObjectMapper objectMapper = ConnectionUtils.DEFAULT_SERIALIZER;
      final TelemetryEvent.LifecycleShutdown shutdownPayload = ImmutableTelemetryEvent.LifecycleShutdown.builder()
          .instanceId(instanceId)
          .metricsTotal(metricsSinceInception)
          .build();
      final String requestBodyJson = objectMapper.writeValueAsString(shutdownPayload);
      final HttpRequest shutdownTelemetryRequest = HttpRequest.newBuilder()
          .uri(ConnectionUtils.getFullUri(
              this.ingestBaseUri, ConnectionUtils.Route.TELEMETRY_LIFECYCLE))
          .header(
              ConnectionUtils.Headers.KEY__CONTENT_TYPE,
              ConnectionUtils.Headers.VAL__CONTENT_JSON)
          .header(
              ConnectionUtils.Headers.KEY__USER_AGENT,
              ConnectionUtils.Headers.getUserAgent(
                  instanceInfo.get().javaVersion(),
                  Optional.of(Dashdive.VERSION),
                  Optional.of(instanceId)))
          .header(ConnectionUtils.Headers.KEY__API_KEY, apiKey)
          .POST(HttpRequest.BodyPublishers.ofString(requestBodyJson))
          .build();
      ConnectionUtils.send(dashdiveHttpClient, shutdownTelemetryRequest);
    } catch (IOException | InterruptedException ignored) {
    }
  }

  private ClientOverrideConfiguration.Builder addInterceptorToIdempotentlyTo(
      final ClientOverrideConfiguration.Builder overrideConfigBuilder) {
    final boolean alreadyHasDashdiveInterceptor = overrideConfigBuilder.executionInterceptors().stream()
        .anyMatch(interceptor -> interceptor instanceof S3RoundTripInterceptor);
    return alreadyHasDashdiveInterceptor
        ? overrideConfigBuilder
        : overrideConfigBuilder.addExecutionInterceptor(this.s3RoundTripInterceptor);
  }

  public ClientOverrideConfiguration.Builder addInterceptorTo(
      final ClientOverrideConfiguration.Builder overrideConfigBuilder) {
    return addInterceptorToIdempotentlyTo(overrideConfigBuilder);
  }

  public S3ClientBuilder addConfigWithInterceptorTo(final S3ClientBuilder clientBuilder) {
    final ClientOverrideConfiguration newOverrideConfig = addInterceptorToIdempotentlyTo(
        ClientOverrideConfiguration.builder()).build();
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
