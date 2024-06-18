package com.dashdive;

import com.dashdive.internal.DashdiveConnection;
import com.dashdive.internal.ImmutableDashdiveInstanceInfo;
import com.dashdive.internal.ImmutableSetupDefaults;
import com.dashdive.internal.SetupDefaults;
import com.dashdive.internal.extraction.S3RoundTripInterceptor;
import com.dashdive.internal.telemetry.EventPipelineMetrics;
import com.dashdive.internal.telemetry.TelemetryPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ClientLifecycleTest {
  @Test
  void eventsRejectedAfterShutdown() {
    final MockHttpClient sharedMockedClient = new MockHttpClient();

    final SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder()
                    .classInstanceId("events-rejected-after-shutdown")
                    .build())
            .targetEventBatchSize(100)
            .startupTelemetryWarnings(TelemetryPayload.of())
            .build();
    final Dashdive dashdive =
        new Dashdive(
            TestUtils.API_KEY_DUMMY,
            Optional.of(TestUtils.FACTORY_CUSTOMER),
            Optional.empty(),
            sharedMockedClient.getDelegate(),
            sharedMockedClient.getDelegate(),
            sharedMockedClient.getDelegate(),
            sharedMockedClient.getDelegate(),
            Optional.of(setupDefaults));

    final S3RoundTripInterceptor interceptor = dashdive.getInterceptorForImperativeTrigger();

    final boolean wasQueuedPreShutdown1 =
        interceptor._afterExecutionReturningSuccess(
            TestUtils.GENERIC_INTERCEPTED_EVENT, TestUtils.EXEC_ATTRS_EMPTY);
    final boolean wasQueuedPreShutdown2 =
        interceptor._afterExecutionReturningSuccess(
            TestUtils.GENERIC_INTERCEPTED_EVENT, TestUtils.EXEC_ATTRS_EMPTY);

    dashdive.close();

    final boolean wasQueuedPostShutdown1 =
        interceptor._afterExecutionReturningSuccess(
            TestUtils.GENERIC_INTERCEPTED_EVENT, TestUtils.EXEC_ATTRS_EMPTY);
    final boolean wasQueuedPostShutdown2 =
        interceptor._afterExecutionReturningSuccess(
            TestUtils.GENERIC_INTERCEPTED_EVENT, TestUtils.EXEC_ATTRS_EMPTY);

    Assertions.assertTrue(wasQueuedPreShutdown1);
    Assertions.assertTrue(wasQueuedPreShutdown2);
    Assertions.assertFalse(wasQueuedPostShutdown1);
    Assertions.assertFalse(wasQueuedPostShutdown2);
  }

  @Test
  void invalidApiKeyReportedOnStartup() {
    final MockHttpClient startupMockedClient = new MockHttpClient();
    final MockHttpClient ignoredMockedClient = new MockHttpClient();

    final Dashdive dashdive =
        new Dashdive(
            "INVALID-KEY",
            Optional.of(TestUtils.FACTORY_CUSTOMER),
            Optional.empty(),
            ignoredMockedClient.getDelegate(),
            startupMockedClient.getDelegate(),
            ignoredMockedClient.getDelegate(),
            ignoredMockedClient.getDelegate(),
            Optional.empty(),
            true);
    dashdive.blockUntilSetupComplete();

    final String fullApiKeyTelemetryPath =
        DashdiveConnection.getRoute(DashdiveConnection.Route.TELEMETRY_API_KEY).getPath();
    List<Optional<String>> matchingRequestBodies =
        startupMockedClient.getRequests().stream()
            .filter(req -> fullApiKeyTelemetryPath.equals(req.request().uri().getPath()))
            .map(req -> req.body())
            .collect(Collectors.toList());
    Assertions.assertEquals(1, matchingRequestBodies.size());
    final Optional<String> maybeRequestBody = matchingRequestBodies.get(0);
    Assertions.assertTrue(maybeRequestBody.isPresent());

    final ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> parsedJson = null;
    try {
      parsedJson =
          objectMapper.readValue(
              maybeRequestBody.get(), new TypeReference<Map<String, Object>>() {});
    } catch (JsonProcessingException exception) {
      Assertions.fail("Failed to parse JSON request body: " + exception.getMessage());
    }

    Assertions.assertInstanceOf(String.class, parsedJson.get("instanceId"));
    Assertions.assertTrue(!((String) parsedJson.get("instanceId")).isEmpty());

    Assertions.assertEquals("INVALID-KEY", parsedJson.getOrDefault("apiKey", ""));
    Assertions.assertInstanceOf(List.class, parsedJson.get("errors"));

    Assertions.assertEquals(1, ((List<?>) parsedJson.get("errors")).size());
    Assertions.assertEquals("invalid_api_key", parsedJson.get("eventType"));

    dashdive.close();
  }

  @Test
  void setupAndShutdownEventsAreReported() {
    final MockHttpClient startupMockedClient = new MockHttpClient();
    final MockHttpClient shutdownMockedClient = new MockHttpClient();
    final MockHttpClient ignoredMockedClient = new MockHttpClient();

    SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder()
                    .classInstanceId("setup-shutdown-reported")
                    .build())
            .targetEventBatchSize(100)
            .startupTelemetryWarnings(TelemetryPayload.of())
            .build();
    final Dashdive dashdive =
        new Dashdive(
            TestUtils.API_KEY_DUMMY,
            Optional.of(TestUtils.FACTORY_CUSTOMER),
            Optional.empty(),
            shutdownMockedClient.getDelegate(),
            startupMockedClient.getDelegate(),
            ignoredMockedClient.getDelegate(),
            ignoredMockedClient.getDelegate(),
            Optional.of(setupDefaults));

    dashdive.blockUntilSetupComplete();
    dashdive.close();

    final String fullLifecycleTelemetryPath =
        DashdiveConnection.getRoute(DashdiveConnection.Route.TELEMETRY_LIFECYCLE).getPath();
    final List<Optional<String>> startupReqBodies =
        startupMockedClient.getRequests().stream()
            .filter(req -> fullLifecycleTelemetryPath.equals(req.request().uri().getPath()))
            .map(req -> req.body())
            .collect(Collectors.toList());
    Assertions.assertEquals(1, startupReqBodies.size());
    Assertions.assertTrue(startupReqBodies.get(0).isPresent());
    final String startupReqBody = startupReqBodies.get(0).get();

    final List<Optional<String>> shutdownReqBodies =
        shutdownMockedClient.getRequests().stream()
            .filter(req -> fullLifecycleTelemetryPath.equals(req.request().uri().getPath()))
            .map(req -> req.body())
            .collect(Collectors.toList());
    Assertions.assertEquals(1, shutdownReqBodies.size());
    Assertions.assertTrue(shutdownReqBodies.get(0).isPresent());
    final String shutdownReqBody = shutdownReqBodies.get(0).get();

    final ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Object> parsedStartupJson = null;
    Map<String, Object> parsedShutdownJson = null;
    try {
      parsedStartupJson =
          objectMapper.readValue(startupReqBody, new TypeReference<Map<String, Object>>() {});
      parsedShutdownJson =
          objectMapper.readValue(shutdownReqBody, new TypeReference<Map<String, Object>>() {});
    } catch (JsonProcessingException exception) {
      Assertions.fail("Failed to parse JSON request body: " + exception.getMessage());
    }

    Assertions.assertEquals("setup-shutdown-reported", parsedStartupJson.get("instanceId"));
    Assertions.assertEquals("startup", parsedStartupJson.get("eventType"));
    Assertions.assertInstanceOf(Map.class, parsedStartupJson.get("instanceInfo"));

    Assertions.assertEquals("setup-shutdown-reported", parsedShutdownJson.get("instanceId"));
    Assertions.assertEquals("shutdown", parsedShutdownJson.get("eventType"));
    Assertions.assertInstanceOf(Map.class, parsedShutdownJson.get("metricsTotal"));
  }

  private static final int GRACE_PERIOD_TESTS_BATCH_SIZE = 2;
  // Max batch processor thread pool size times batch size times 2
  private static final int GRACE_PERIOD_TESTS_EVENT_COUNT = 10 * 2 * 2;
  private static final int GRACE_PERIOD_TESTS_BATCH_COUNT =
      GRACE_PERIOD_TESTS_EVENT_COUNT / GRACE_PERIOD_TESTS_BATCH_SIZE;

  @Value.Immutable
  public abstract static class ShutdownTestCommonResult {
    public abstract int sentBatchCount();

    public abstract Map<String, Integer> totalMetrics();

    public abstract Map<String, Integer> combinedIncrementalMetrics();
  }

  private static ShutdownTestCommonResult runShutdownTestCommonLogic(
      Optional<Duration> shutdownGracePeriod, String instanceId) {
    final MockHttpClient batchesMockedClient =
        new MockHttpClient(
            Optional.of(
                () -> {
                  try {
                    Thread.sleep(10);
                  } catch (InterruptedException ignored) {
                  }
                }));
    final MockHttpClient incrementalMetricsMockedClient = new MockHttpClient();
    final MockHttpClient totalMetricsMockedClient = new MockHttpClient();
    final MockHttpClient ignoredMockedClient = new MockHttpClient();

    final SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder().classInstanceId(instanceId).build())
            .targetEventBatchSize(GRACE_PERIOD_TESTS_BATCH_SIZE)
            .startupTelemetryWarnings(TelemetryPayload.of())
            .build();
    final Dashdive dashdive =
        new Dashdive(
            TestUtils.API_KEY_DUMMY,
            Optional.of(TestUtils.FACTORY_CUSTOMER),
            shutdownGracePeriod,
            totalMetricsMockedClient.getDelegate(),
            ignoredMockedClient.getDelegate(),
            batchesMockedClient.getDelegate(),
            incrementalMetricsMockedClient.getDelegate(),
            Optional.of(setupDefaults));
    dashdive.blockUntilSetupComplete();

    final S3RoundTripInterceptor interceptor = dashdive.getInterceptorForImperativeTrigger();

    for (int i = 0; i < GRACE_PERIOD_TESTS_EVENT_COUNT; i++) {
      interceptor.afterExecution(TestUtils.GENERIC_INTERCEPTED_EVENT, TestUtils.EXEC_ATTRS_EMPTY);
    }

    try {
      Thread.sleep(50);
    } catch (InterruptedException ignored) {
    }
    dashdive.close();
    dashdive.blockUntilShutdownComplete();

    final List<String> batchesReqBodies = batchesMockedClient.unboxRequestBodiesAssertingNonempty();
    final List<String> incrementalMetricsReqBodies =
        incrementalMetricsMockedClient.unboxRequestBodiesAssertingNonempty();
    final List<Map<String, Integer>> incrementalMetrics =
        incrementalMetricsReqBodies.stream()
            .map(
                metricReqBody ->
                    MetricsUtils.getMetricsFromRequestBodyWithAssertions(
                        MetricsUtils.Type.INCREMENTAL, metricReqBody, instanceId))
            .collect(Collectors.toList());
    final Map<String, Integer> combinedIncrementalMetrics =
        MetricsUtils.getCombinedMetricsFromIncremental(incrementalMetrics);

    Assertions.assertEquals(1, totalMetricsMockedClient.getRequests().size());
    final String totalMetricsReqBody =
        totalMetricsMockedClient.unboxRequestBodiesAssertingNonempty().get(0);
    final Map<String, Integer> totalMetrics =
        MetricsUtils.getMetricsFromRequestBodyWithAssertions(
            MetricsUtils.Type.TOTAL, totalMetricsReqBody, instanceId);

    Assertions.assertEquals(
        GRACE_PERIOD_TESTS_EVENT_COUNT,
        totalMetrics.get(EventPipelineMetrics.Type.EVENTS_ENQUEUED.toWireString()));
    Assertions.assertEquals(
        batchesReqBodies.size(),
        totalMetrics.get(EventPipelineMetrics.Type.BATCHES_SENT.toWireString()));

    return ImmutableShutdownTestCommonResult.builder()
        .combinedIncrementalMetrics(combinedIncrementalMetrics)
        .totalMetrics(totalMetrics)
        .sentBatchCount(batchesReqBodies.size())
        .build();
  }

  // NOTE: Also tests that incremental and total metrics are sent and that
  // they line up with the actual events sent
  @Test
  void shutdownGracePeriodCanDropEvents() {
    final ShutdownTestCommonResult shutdownTestCommonResult =
        runShutdownTestCommonLogic(Optional.of(Duration.ZERO), "shutdown-YES-grace-period");

    final int sentBatchCount = shutdownTestCommonResult.sentBatchCount();
    Assertions.assertTrue(GRACE_PERIOD_TESTS_BATCH_COUNT > sentBatchCount && sentBatchCount > 0);

    final Map<String, Integer> totalMetrics = shutdownTestCommonResult.totalMetrics();
    final Map<String, Integer> combinedIncrementalMetrics =
        shutdownTestCommonResult.combinedIncrementalMetrics();
    Assertions.assertEquals(totalMetrics.keySet(), combinedIncrementalMetrics.keySet());
    // It may be the case that some incremental metrics were dropped because the grace period
    // ended in the middle of sending a batch. For such metrics, we check ">=" rather than "=".
    // But, as in the `runShutdownTestCommonLogic` function assertions, `totalMetrics` can
    // always be treated as a source of truth.
    Assertions.assertTrue(
        totalMetrics.keySet().stream()
            .allMatch(
                key -> {
                  if (key.equals(EventPipelineMetrics.Type.EVENTS_SENT.toWireString())
                      || key.equals(EventPipelineMetrics.Type.BATCHES_SENT.toWireString())) {
                    return totalMetrics.get(key) >= combinedIncrementalMetrics.get(key);
                  }
                  return totalMetrics.get(key) >= combinedIncrementalMetrics.get(key);
                }));
  }

  // NOTE: Also tests that incremental and total metrics are sent and that
  // they line up with the actual events sent
  @Test
  void fullShutdownNeverDropsEvents() {
    final ShutdownTestCommonResult shutdownTestCommonResult =
        runShutdownTestCommonLogic(Optional.empty(), "shutdown-NO-grace-period");

    final int sentBatchCount = shutdownTestCommonResult.sentBatchCount();
    Assertions.assertEquals(GRACE_PERIOD_TESTS_BATCH_COUNT, sentBatchCount);

    final Map<String, Integer> totalMetrics = shutdownTestCommonResult.totalMetrics();
    final Map<String, Integer> combinedIncrementalMetrics =
        shutdownTestCommonResult.combinedIncrementalMetrics();
    Assertions.assertEquals(totalMetrics, combinedIncrementalMetrics);
  }
}
