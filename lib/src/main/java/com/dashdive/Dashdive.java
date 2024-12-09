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

/**
 * An instance of the Dashdive collector, which is used to instrument one or multiple AWS {@link
 * software.amazon.awssdk.services.s3.S3Client} instances. Each instance of this class maintains a
 * separate connection to the Dashdive ingestion API, a separate event processing pipeline with
 * multiple thread pools, and a separate metrics collector. In practice, most users will only need
 * to instantiate this class once per application lifecycle.
 */
@Value.Style(newBuilder = "builder")
public class Dashdive implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(Dashdive.class);
  public static final URI DEFAULT_INGEST_BASE_URI = URI.create("https://ingest.dashdive.com");

  /** The version of the Dashdive SDK. */
  public static final String VERSION = "1.0.0";

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
  // Helps us understand any unexpected modifications pre- or post- Dashdive data collection:
  // "s3Client.serviceClientConfiguration().overrideConfiguration().executionInterceptors()
  //              .forEach(System.out::println);"

  // Constructors are package-private instead of fully private for testing,
  // to allow for mocked HTTP clients
  Dashdive(
      URI ingestBaseUri,
      String apiKey,
      Optional<S3EventAttributeExtractor> s3EventAttributeExtractor,
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

    this.ingestBaseUri = ingestBaseUri;
    this.apiKey = apiKey;
    if (!s3EventAttributeExtractor.isPresent()) {
      logger.warn(
          "No S3 event attribute extractor factory provided; using factory for no-op extractor.");
    }
    final S3EventAttributeExtractor presentS3EventAttributeExtractor =
        s3EventAttributeExtractor.orElse(new NoOpS3EventAttributeExtractor());
    this.batchEventProcessor =
        new BatchEventProcessor(
            this.instanceInfo,
            apiKey,
            ingestBaseUri,
            presentS3EventAttributeExtractor,
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
            ingestBaseUri,
            skipSetupWithDefaults,
            shouldSkipImdsQueries,
            isInitialized,
            instanceInfo,
            targetEventBatchSize,
            Optional.of(() -> this.batchEventProcessor.notifyInitialized()));
    this.initialSetupWorkerThread = new Thread(this.initialSetupWorker);
    this.initialSetupWorkerThread.start();
    this.isShutDown = new AtomicBoolean(false);

    logger.info("Dashdive instance created with id: {}", instanceId);
  }

  Dashdive(
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
        dashdiveHttpClient,
        setupHttpClient,
        batchProcessorHttpClient,
        metricsHttpClient,
        skipSetupWithDefaultss,
        false);
  }

  Dashdive(
      URI ingestionBaseUri,
      String apiKey,
      Optional<S3EventAttributeExtractor> s3EventAttributeExtractor,
      Optional<Duration> shutdownGracePeriod) {
    this(Optional.of(ingestionBaseUri), Optional.of(apiKey),
      s3EventAttributeExtractor, shutdownGracePeriod);
  }

  /**
   * Construct a new {@link Dashdive} instance with the provided API key, S3 event attribute
   * extractor factory, and shutdown grace period.
   *
   * @param apiKey the API key to use for sending telemetry data
   * @param s3EventAttributeExtractor a factory which returns a function which extracts attributes
   *     from S3 events
   * @param shutdownGracePeriod the duration to wait for the Dashdive instance to flush any
   *     remaining events and send
   */
  @Builder.Constructor
  Dashdive(
      // All fields are optional to avoid NullPointerExceptions at runtime;
      // much better to have sends simply fail, for example, when apiKey is not specified
      Optional<URI> ingestionBaseUri,
      Optional<String> apiKey,
      Optional<S3EventAttributeExtractor> s3EventAttributeExtractor,
      Optional<Duration> shutdownGracePeriod) {
    // No need to check the user-supplied values for null, since the Immutables
    // builder automatically enforces non-null
    this(
        ingestionBaseUri.orElse(DEFAULT_INGEST_BASE_URI),
        apiKey.orElse(""),
        s3EventAttributeExtractor,
        shutdownGracePeriod,
        ConnectionUtils.directExecutorHttpClient(),
        ConnectionUtils.directExecutorHttpClient(),
        ConnectionUtils.directExecutorHttpClient(),
        ConnectionUtils.directExecutorHttpClient(),
        Optional.empty(),
        false);
  }

  /**
   * Construct a new {@link DashdiveBuilder} instance, which is used to configure and create a new
   * {@link Dashdive} instance.
   *
   * @return a new {@link DashdiveBuilder} instance
   */
  public static DashdiveBuilder builder() {
    return DashdiveBuilder.builder();
  }

  /**
   * Close the Dashdive instance, flushing any remaining events that have been collected. This
   * method is idempotent.
   *
   * <p>This immediately prevents any further events from being collected. Events previously
   * collected, but not yet sent, will block the close operation either for the duration of the
   * grace period, or indefinitely if no grace period was set. For reference, see {@link
   * DashdiveBuilder#shutdownGracePeriod(Duration)}.
   *
   * <p><b>NOTE</b>: This class is {@link AutoCloseable}, so in many instances, such as in Spring
   * projects, this method need not be called explicitly.
   */
  public void close() {
    // Requiring an explicit `close` call is OK in certain circumstances and has
    // precedence with other robust SDKs, such as the AWS SDK:
    // https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/imds/Ec2MetadataClient.html#closing-the-client-heading
    //
    // Additionally, `close` is auto-inferred by Spring as the default destroy method
    // (auto-called at end of program). See: https://stackoverflow.com/a/44757112/14816795
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

    final ImmutableMap<String, Integer> metricsSinceInception =
        this.batchEventProcessor.getSerializableMetricsSinceInception();
    logger.info("Dashdive instance shutting down. Lifetime metrics: {}", metricsSinceInception);

    try {
      final ObjectMapper objectMapper = ConnectionUtils.DEFAULT_SERIALIZER;
      final TelemetryEvent.LifecycleShutdown shutdownPayload =
          ImmutableTelemetryEvent.LifecycleShutdown.builder()
              .instanceId(instanceId)
              .metricsTotal(metricsSinceInception)
              .build();
      final String requestBodyJson = objectMapper.writeValueAsString(shutdownPayload);
      final HttpRequest shutdownTelemetryRequest =
          HttpRequest.newBuilder()
              .uri(ConnectionUtils.getFullUri(
                  this.ingestBaseUri, ConnectionUtils.Route.TELEMETRY_LIFECYCLE))
              .header(
                  ConnectionUtils.Headers.KEY__CONTENT_TYPE,
                  ConnectionUtils.Headers.VAL__CONTENT_JSON)
              .header(
                  ConnectionUtils.Headers.KEY__USER_AGENT,
                  ConnectionUtils.Headers.getUserAgent(
                      instanceInfo.get().javaVersion(),
                      Optional.of(VERSION),
                      Optional.of(instanceId)))
              .header(ConnectionUtils.Headers.KEY__API_KEY, apiKey)
              .POST(HttpRequest.BodyPublishers.ofString(requestBodyJson))
              .build();
      ConnectionUtils.send(dashdiveHttpClient, shutdownTelemetryRequest);
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

  /**
   * Add the Dashdive S3 event interceptor to the provided configuration builder. This method is
   * idempotent, so calling it multiple times on the same configuration builder will add the
   * interceptor exactly once.
   *
   * @param overrideConfigBuilder the configuration builder to add the interceptor to
   * @return the provided configuration builder with the Dashdive S3 event interceptor added
   */
  public ClientOverrideConfiguration.Builder addInterceptor(
      final ClientOverrideConfiguration.Builder overrideConfigBuilder) {
    return addInterceptorIdempotentlyTo(overrideConfigBuilder);
  }

  /**
   * Add the Dashdive S3 event interceptor to the provided S3 client builder. This method creates a
   * new {@link ClientOverrideConfiguration} with only the Dashdive S3 event interceptor, and sets
   * it on the provided client builder. It is a convenience method; in other words, the following
   * are equivalent:
   *
   * <pre>{@code
   * // Convenient version
   * S3Client s3Client = dashdive.addConfigWithInterceptor(S3Client.builder()).build()
   *
   * // Verbose version
   * S3Client s3Client = S3Client.builder()
   *      .overrideConfiguration(
   *            dashdive.addInterceptor(ClientOverrideConfiguration.builder()).build())
   *      .build()
   * }</pre>
   *
   * <b>NOTE</b>:
   *
   * <ul>
   *   <li>This method creates an entirely new {@link ClientOverrideConfiguration}, so any existing
   *       override configuration on the client builder will be erased, if previously set.
   *   <li>Once this method is called on the client builder, subsequent calls to {@link
   *       S3ClientBuilder#overrideConfiguration(ClientOverrideConfiguration)} will erase the
   *       interceptor.
   * </ul>
   *
   * @param clientBuilder the S3 client builder to add the interceptor to
   * @return the provided S3 client builder with the Dashdive S3 event interceptor added
   */
  public S3ClientBuilder addConfigWithInterceptor(final S3ClientBuilder clientBuilder) {
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
