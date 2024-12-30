package com.dashdive.internal.telemetry;

import com.dashdive.internal.ConnectionUtils;
import com.dashdive.internal.DashdiveInstanceInfo;
import com.dashdive.internal.PriorityThreadFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class EventPipelineMetrics {
  private final ImmutableMap<Type, Metric> metrics;
  private final Lock metricsLock;

  private static final int EXECUTOR_CORE_POOL_SIZE = 1;
  private final ScheduledThreadPoolExecutor periodicSender;
  private Optional<ScheduledFuture<Void>> periodicSenderFuture;
  private static final long DEFAULT_MAX_INCREMENTAL_METRICS_AGE_MS = 10 * 60 * 1_000;

  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  private final String userAgent;
  private final AtomicReference<DashdiveInstanceInfo> instanceInfo;
  private final URI ingestBaseUri;
  private final String apiKey;
  private final long maxIncrementalMetricsDelayMs;
  private final Optional<Supplier<Boolean>> disableAllTelemetrySupplier;

  public EventPipelineMetrics(
      AtomicReference<DashdiveInstanceInfo> instanceInfo, String apiKey,
      URI ingestBaseUri, HttpClient httpClient, Optional<Duration> maxIncrementalMetricsDelay,
      Optional<Supplier<Boolean>> disableAllTelemetrySupplier) {
    this.metrics =
        Stream.of(Type.values()).collect(ImmutableMap.toImmutableMap(k -> k, k -> new Metric()));
    this.metricsLock = new ReentrantLock();
    this.disableAllTelemetrySupplier = disableAllTelemetrySupplier;

    this.periodicSender = new ScheduledThreadPoolExecutor(
        EXECUTOR_CORE_POOL_SIZE, new PriorityThreadFactory(Thread.MIN_PRIORITY));
    this.periodicSender.setExecuteExistingDelayedTasksAfterShutdownPolicy(true);
    this.periodicSender.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    this.periodicSenderFuture = Optional.empty();

    this.objectMapper = ConnectionUtils.DEFAULT_SERIALIZER;

    this.httpClient = httpClient;
    this.userAgent = ConnectionUtils.Headers.getUserAgentFromInstanceInfo(instanceInfo.get());
    this.instanceInfo = instanceInfo;
    this.apiKey = apiKey;
    this.ingestBaseUri = ingestBaseUri;
    this.maxIncrementalMetricsDelayMs = maxIncrementalMetricsDelay
            .map(d -> d.toMillis()).orElse(DEFAULT_MAX_INCREMENTAL_METRICS_AGE_MS);
  }

  private Void sendIncrementalMetrics() {
    final boolean shouldDisableTelemetry = disableAllTelemetrySupplier.map(s -> s.get()).orElse(false);
    if (shouldDisableTelemetry) {
      return null;
    }

    metricsLock.lock();
    ImmutableMap<String, Integer> incrementalMetricsPayload;
    try {
      incrementalMetricsPayload =
          metrics.entrySet().stream()
              .collect(
                  ImmutableMap.toImmutableMap(
                      e -> e.getKey().toWireString(), e -> e.getValue().getValueSinceLastSend()));
      metrics.values().forEach(Metric::recordSend);
      // Optimistically mark task as done
      periodicSenderFuture = Optional.empty();
    } finally {
      metricsLock.unlock();
    }
    if (incrementalMetricsPayload.values().stream().allMatch(v -> v == 0)) {
      return null;
    }

    try {
      final TelemetryEvent.MetricsIncremental metricsPayload =
          ImmutableTelemetryEvent.MetricsIncremental.builder()
              .instanceId(instanceInfo.get().classInstanceId().orElse(""))
              .metricsIncremental(incrementalMetricsPayload)
              .build();
      final String requestBodyJson = objectMapper.writeValueAsString(metricsPayload);
      final HttpRequest metricsRequest =
          HttpRequest.newBuilder()
              .uri(ConnectionUtils.getFullUri(
                  this.ingestBaseUri, ConnectionUtils.Route.TELEMETRY_METRICS))
              .header(
                  ConnectionUtils.Headers.KEY__CONTENT_TYPE,
                  ConnectionUtils.Headers.VAL__CONTENT_JSON)
              .header(ConnectionUtils.Headers.KEY__USER_AGENT, userAgent)
              .header(ConnectionUtils.Headers.KEY__API_KEY, apiKey)
              .POST(HttpRequest.BodyPublishers.ofString(requestBodyJson))
              .build();
      ConnectionUtils.send(httpClient, metricsRequest);
    } catch (IOException exception) {
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
    return null;
  }

  public void shutDownAndFlush() {
    periodicSenderFuture.ifPresent(future -> future.cancel(true));
    periodicSender.shutdownNow();
    try {
      periodicSender.awaitTermination(1, TimeUnit.DAYS);
    } catch (InterruptedException exception) {
      Thread.currentThread().interrupt();
    }
    if (periodicSenderFuture.isEmpty() || periodicSenderFuture.get().isCancelled()) {
      sendIncrementalMetrics();
    }
  }

  public void add(Type type, int quantity) {
    if (quantity == 0) {
      return;
    }

    metricsLock.lock();
    try {
      metrics.get(type).add(quantity);
      final boolean noSenderFuture =
          periodicSenderFuture.isEmpty() || periodicSenderFuture.get().isDone();
      if (noSenderFuture && !periodicSender.isShutdown()) {
        periodicSenderFuture =
            Optional.of(
                periodicSender.schedule(
                    this::sendIncrementalMetrics,
                    maxIncrementalMetricsDelayMs,
                    TimeUnit.MILLISECONDS));
      }
    } finally {
      metricsLock.unlock();
    }
  }

  public void increment(Type type) {
    add(type, 1);
  }

  public void addAll(ImmutableMap<Type, Integer> quantitiesByMetric) {
    metricsLock.lock();
    try {
      quantitiesByMetric.forEach(this::add);
    } finally {
      metricsLock.unlock();
    }
  }

  public ImmutableMap<String, Integer> getSerializableMetricsSinceInception() {
    metricsLock.lock();
    try {
      return metrics.entrySet().stream()
          .collect(
              ImmutableMap.toImmutableMap(
                  e -> e.getKey().toWireString(), e -> e.getValue().getValueSinceInception()));
    } finally {
      metricsLock.unlock();
    }
  }

  // These metrics are "Counts" only, not "Gauges" or other metric types.
  // See: https://prometheus.io/docs/tutorials/understanding_metric_types/
  // We may add support for gauges in the future (e.g., queue size or latency).
  private class Metric {
    private int valueSinceInception;
    private int valueSinceLastSend;

    public Metric() {
      this.valueSinceInception = 0;
      this.valueSinceLastSend = 0;
    }

    public void add(int quantity) {
      valueSinceInception += quantity;
      valueSinceLastSend += quantity;
    }

    public void recordSend() {
      valueSinceLastSend = 0;
    }

    public int getValueSinceInception() {
      return valueSinceInception;
    }

    public int getValueSinceLastSend() {
      return valueSinceLastSend;
    }
  }

  // NOTE 1: *_ENQUEUED should always equal *_SENT
  // NOTE 2: The total number of encountered items is given by *_ENQUEUED + *_DROPPED_FROM_QUEUE
  public enum Type {
    // Only includes events successfully enqueued, not those dropped
    EVENTS_ENQUEUED("events_enqueued"),
    EVENTS_SENT("events_sent"),
    // "Dropped from queue" includes both cases where queue is full
    // and cases where the executor has been shut down previously
    EVENTS_DROPPED_FROM_QUEUE("events_dropped_from_queue"),
    // "Send failure" only includes events that failed to send but were NOT
    // pruned due to having irrecoverable errors (e.g., malformed event).
    // This also counts and includes S3 client failures (e.g. 4xx, 3xx).
    EVENTS_DROPPED_SEND_FAILURE("events_dropped_send_failure"),
    EVENTS_DROPPED_PARSE_ERROR("events_dropped_parse_error"),

    BATCHES_ENQUEUED("batches_enqueued"),
    BATCHES_SENT("batches_sent"),
    BATCHES_DROPPED_FROM_QUEUE("batches_dropped_from_queue"),
    BATCHES_DROPPED_SEND_FAILURE("batches_dropped_send_failure"),
    BATCHES_DROPPED_PARSE_ERROR("batches_dropped_parse_error"),

    EVENTS_WITH_WARNINGS("events_with_warnings"),
    EVENTS_WITH_ERRORS("events_with_errors");

    private final String text;

    Type(final String text) {
      this.text = text;
    }

    @Override
    public String toString() {
      return text;
    }

    public String toWireString() {
      return text;
    }
  }

  @VisibleForTesting
  public void _blockUntilShutdownComplete() throws InterruptedException {
    periodicSender.awaitTermination(1, TimeUnit.DAYS);
  }
}
