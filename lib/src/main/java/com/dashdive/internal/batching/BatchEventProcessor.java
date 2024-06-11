package com.dashdive.internal.batching;

import com.dashdive.Dashdive;
import com.dashdive.S3EventAttributeExtractor;
import com.dashdive.S3EventAttributeExtractorFactory;
import com.dashdive.internal.DashdiveConnection;
import com.dashdive.internal.DashdiveConnection.BackoffSendConfig;
import com.dashdive.internal.DashdiveInstanceInfo;
import com.dashdive.internal.ImmutableBackoffSendConfig;
import com.dashdive.internal.S3SingleExtractedEvent;
import com.dashdive.internal.telemetry.EventPipelineMetrics;
import com.dashdive.internal.telemetry.ImmutableTelemetryEvent;
import com.dashdive.internal.telemetry.TelemetryEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PausableThreadPoolExecutor extends ThreadPoolExecutor {
  private boolean isPaused;
  private ReentrantLock pauseLock = new ReentrantLock();
  private Condition unpaused = pauseLock.newCondition();

  public PausableThreadPoolExecutor(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    isPaused = false;
  }

  protected void beforeExecute(Thread t, Runnable r) {
    super.beforeExecute(t, r);
    pauseLock.lock();
    try {
      while (isPaused) {
        unpaused.await();
      }
    } catch (InterruptedException ie) {
      t.interrupt();
    } finally {
      pauseLock.unlock();
    }
  }

  public void pause() {
    pauseLock.lock();
    try {
      isPaused = true;
    } finally {
      pauseLock.unlock();
    }
  }

  public void resume() {
    pauseLock.lock();
    try {
      isPaused = false;
      unpaused.signalAll();
    } finally {
      pauseLock.unlock();
    }
  }
}

public class BatchEventProcessor {
  private static final Logger logger = LoggerFactory.getLogger(BatchEventProcessor.class);

  private final EventPipelineMetrics metrics;
  private final HttpClient httpClient;
  private String userAgent;
  private final String apiKey;
  private final ObjectMapper objectMapper;

  private final AtomicReference<DashdiveInstanceInfo> instanceInfo;

  private final S3EventAttributeExtractorFactory s3EventAttributeExtractorFactory;
  private final Optional<Duration> shutdownGracePeriod;

  private static final int EXECUTOR_CORE_POOL_SIZE = 0;
  private static final int EXECUTOR_MAX_POOL_SIZE = 10;
  private static final long EXECUTOR_KEEP_ALIVE_TIME_MS = 1000;
  // Assuming an average batch size of 100 events, corresponds to roughly 20 MB of heap usage
  private static final int EXECUTOR_QUEUE_MAX_BATCHES = 100;
  // By using head and tail pointers, insertion and removal are constant time. See:
  // https://github.com/openjdk/jdk21u-dev/blob/master/src/java.base/share/classes/java/util/concurrent/ArrayBlockingQueue.java#L179
  private final BlockingQueue<Runnable> executorQueue;
  private final PausableThreadPoolExecutor executor;

  public BatchEventProcessor(
      AtomicReference<DashdiveInstanceInfo> instanceInfo,
      String apiKey,
      S3EventAttributeExtractorFactory s3EventAttributeExtractorFactory,
      Optional<Duration> shutdownGracePeriod,
      // TODO: It may be the case that Java 11's HttpClient is not thread safe,
      // or the SSL context is not thread safe (see: https://stackoverflow.com/a/53767728),
      // or at a minimum we may be doing extra work since HttpClientImpls seem to have their
      // own manager threads.
      HttpClient batchProcessorHttpClient,
      HttpClient metricsHttpClient) {
    this.metrics = new EventPipelineMetrics(instanceInfo, apiKey, metricsHttpClient);
    this.httpClient = batchProcessorHttpClient;
    this.userAgent = "";
    this.objectMapper = DashdiveConnection.DEFAULT_SERIALIZER;

    this.apiKey = apiKey;
    this.s3EventAttributeExtractorFactory = s3EventAttributeExtractorFactory;
    this.shutdownGracePeriod = shutdownGracePeriod;

    this.instanceInfo = instanceInfo;

    this.executorQueue = new ArrayBlockingQueue<>(EXECUTOR_QUEUE_MAX_BATCHES);
    this.executor =
        new PausableThreadPoolExecutor(
            EXECUTOR_CORE_POOL_SIZE,
            EXECUTOR_MAX_POOL_SIZE,
            EXECUTOR_KEEP_ALIVE_TIME_MS,
            TimeUnit.MILLISECONDS,
            this.executorQueue);
    this.executor.pause();
  }

  private void initializeUserAgent() {
    DashdiveInstanceInfo instanceInfo = this.instanceInfo.get();
    userAgent =
        DashdiveConnection.Headers.getUserAgent(
            instanceInfo.javaVersion(),
            Optional.of(Dashdive.VERSION),
            instanceInfo.classInstanceId());
  }

  public void notifyInitialized() {
    initializeUserAgent();
    executor.resume();
  }

  public void enqueueOrDropBatch(List<S3SingleExtractedEvent> batch) {
    if (batch.size() == 0) {
      logger.warn("Attempted to enqueue empty batch of events");
      return;
    }

    boolean didEnqueue = true;
    try {
      executor.submit(
          () -> processBatch(batch, this.s3EventAttributeExtractorFactory.createExtractor()));
    } catch (RejectedExecutionException exception) {
      didEnqueue = false;
    }

    // `executor.submit()` indeed requires acquisition of the lock on the underlying queue. See:
    // https://github.com/openjdk/jdk21u-dev/blob/master/src/java.base/share/classes/java/util/concurrent/ArrayBlockingQueue.java#L341
    // https://github.com/openjdk/jdk21u-dev/blob/master/src/java.base/share/classes/java/util/concurrent/ThreadPoolExecutor.java#L1368
    // So it's no significant perf hit for us to atomically operate on the metrics object here.
    final int batchSize = batch.size();
    if (didEnqueue) {
      metrics.addAll(
          ImmutableMap.of(
              EventPipelineMetrics.Type.EVENTS_ENQUEUED,
              batchSize,
              EventPipelineMetrics.Type.BATCHES_ENQUEUED,
              batchSize > 0 ? 1 : 0));
    } else {
      logger.warn("Dropped batch of {} events due to full queue", batchSize);
      metrics.addAll(
          ImmutableMap.of(
              EventPipelineMetrics.Type.EVENTS_DROPPED_FROM_QUEUE,
              batchSize,
              EventPipelineMetrics.Type.BATCHES_DROPPED_FROM_QUEUE,
              batchSize > 0 ? 1 : 0));
    }
  }

  public ImmutableMap<String, Integer> getSerializableMetricsSinceInception() {
    return metrics.getSerializableMetricsSinceInception();
  }

  public void shutDownAndFlush() {
    shutDownAndFlushExecutor();
    metrics.shutDownAndFlush();
  }

  private void shutDownAndFlushExecutor() {
    // NOTE: Checked in OpenJDK source to confirm that calling `shutdown()` still executes
    // tasks that have been enqueued, even if they haven't yet been assigned to a worker thread.
    if (shutdownGracePeriod.isEmpty()) {
      // If no grace period, we wait indefinitely for all tasks to complete
      executor.shutdown();
      try {
        executor.awaitTermination(1, TimeUnit.DAYS);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
      }
      return;
    }

    try {
      executor.shutdown();
      if (!executor.awaitTermination(shutdownGracePeriod.get().toMillis(), TimeUnit.MILLISECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException exception) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  private static final int MAX_RETRIES = 3;
  private static final Duration INITIAL_BACKOFF = Duration.ofMillis(200);
  private static final int BACKOFF_MULTIPLIER_NUMERATOR = 5;
  private static final int BACKOFF_MULTIPLIER_DENOMINATOR = 2;
  private static final BackoffSendConfig BACKOFF_CONFIG =
      ImmutableBackoffSendConfig.builder()
          .initialBackoff(INITIAL_BACKOFF)
          .maxTries(MAX_RETRIES)
          .backoffMultiplierNumerator(BACKOFF_MULTIPLIER_NUMERATOR)
          .backoffMultiplierDenominator(BACKOFF_MULTIPLIER_DENOMINATOR)
          .build();

  private void processBatch(
      List<S3SingleExtractedEvent> batch,
      @Nullable S3EventAttributeExtractor s3EventAttributeExtractor) {
    try {
      processBatchUnsafe(batch, s3EventAttributeExtractor);
    } catch (Exception exception) {
      logger.error("Failed to process batch of {} events", batch.size(), exception);
    }
  }

  private void processBatchUnsafe(
      List<S3SingleExtractedEvent> batch,
      @Nullable S3EventAttributeExtractor s3EventAttributeExtractor) {
    final ImmutableList<S3SingleExtractedEvent> finalizedBatch =
        batch.stream()
            .map(
                event ->
                    S3SingleEventProcessor.processAnyEvent(
                        event, s3EventAttributeExtractor, instanceInfo.get().imdRegion()))
            .collect(ImmutableList.toImmutableList());

    final ImmutableList<ImmutableMap<String, Object>> validEvents =
        finalizedBatch.stream()
            .filter(event -> !event.hasIrrecoverableErrors())
            .map(event -> event.dataPayload())
            .collect(ImmutableList.toImmutableList());

    boolean didValidBatchRequestSucceed = false;
    if (validEvents.size() > 0) {
      try {
        final String sendBatchBodyJson = objectMapper.writeValueAsString(validEvents);
        final HttpRequest sendBatchRequest =
            HttpRequest.newBuilder()
                .uri(DashdiveConnection.getRoute(DashdiveConnection.Route.S3_BATCH_INGEST))
                .header(DashdiveConnection.Headers.USER_AGENT, userAgent)
                .header(DashdiveConnection.Headers.API_KEY, apiKey)
                .POST(HttpRequest.BodyPublishers.ofString(sendBatchBodyJson))
                .build();

        final HttpResponse<String> sendBatchResponse =
            DashdiveConnection.sendWithExponentialBackoff(
                httpClient, sendBatchRequest, BACKOFF_CONFIG);
        didValidBatchRequestSucceed = sendBatchResponse.statusCode() == HttpURLConnection.HTTP_OK;
      } catch (IOException exception) {
        didValidBatchRequestSucceed = false;
        logger.error("Failed to send batch of events", exception);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    ImmutableList<S3SingleExtractedEvent> eventsWithTelemetry =
        finalizedBatch.stream()
            .filter(
                event ->
                    event.telemetryErrors().getItems().size() > 0
                        || event.telemetryWarnings().getItems().size() > 0)
            .collect(ImmutableList.toImmutableList());
    if (eventsWithTelemetry.size() > 0) {
      try {
        final TelemetryEvent.ExtractionIssues extractionIssuesPayload =
            ImmutableTelemetryEvent.ExtractionIssues.builder()
                .instanceId(instanceInfo.get().classInstanceId().orElse(""))
                .addAllEventsWithIssues(eventsWithTelemetry)
                .build();
        final String sendTelemetryBodyJson =
            objectMapper.writeValueAsString(extractionIssuesPayload);
        final HttpRequest sendTelemetryRequest =
            HttpRequest.newBuilder()
                .uri(
                    DashdiveConnection.getRoute(
                        DashdiveConnection.Route.TELEMETRY_EXTRACTION_ISSUES))
                .header(DashdiveConnection.Headers.USER_AGENT, userAgent)
                .header(DashdiveConnection.Headers.API_KEY, apiKey)
                .POST(HttpRequest.BodyPublishers.ofString(sendTelemetryBodyJson))
                .build();
        DashdiveConnection.sendWithExponentialBackoff(
            httpClient, sendTelemetryRequest, BACKOFF_CONFIG);
      } catch (IOException exception) {
        logger.error("Failed to send batch of telemetry", exception);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return;
      }
    }

    final int eventsWithWarnings =
        (int)
            eventsWithTelemetry.stream()
                .filter(e -> e.telemetryWarnings().getItems().size() > 0)
                .count();
    final int eventsWithErrors =
        (int)
            eventsWithTelemetry.stream()
                .filter(e -> e.telemetryErrors().getItems().size() > 0)
                .count();
    EventPipelineMetrics.Type eventNetworkType =
        didValidBatchRequestSucceed
            ? EventPipelineMetrics.Type.EVENTS_SENT
            : EventPipelineMetrics.Type.EVENTS_DROPPED_SEND_FAILURE;
    EventPipelineMetrics.Type batchNetworkType =
        didValidBatchRequestSucceed
            ? EventPipelineMetrics.Type.BATCHES_SENT
            : EventPipelineMetrics.Type.BATCHES_DROPPED_SEND_FAILURE;

    metrics.addAll(
        ImmutableMap.of(
            eventNetworkType,
            validEvents.size(),
            batchNetworkType,
            validEvents.size() > 0 ? 1 : 0,
            EventPipelineMetrics.Type.EVENTS_DROPPED_PARSE_ERROR,
            finalizedBatch.size() - validEvents.size(),
            EventPipelineMetrics.Type.BATCHES_DROPPED_PARSE_ERROR,
            validEvents.size() == 0 ? 1 : 0,
            EventPipelineMetrics.Type.EVENTS_WITH_WARNINGS,
            eventsWithWarnings,
            EventPipelineMetrics.Type.EVENTS_WITH_ERRORS,
            eventsWithErrors));
  }

  @VisibleForTesting
  public void _blockUntilShutdownComplete() throws InterruptedException {
    executor.awaitTermination(1, TimeUnit.DAYS);
    metrics._blockUntilShutdownComplete();
  }
}
