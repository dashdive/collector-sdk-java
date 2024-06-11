package com.dashdive;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.dashdive.internal.ImmutableDashdiveInstanceInfo;
import com.dashdive.internal.ImmutableSetupDefaults;
import com.dashdive.internal.SetupDefaults;
import com.dashdive.internal.extraction.S3RoundTripInterceptor;
import com.dashdive.internal.telemetry.EventPipelineMetrics;
import com.dashdive.internal.telemetry.TelemetryPayload;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.InterceptorContext;

class MockEventSender implements Runnable {
  private final S3RoundTripInterceptor interceptor;
  private final int eventCount;
  private final int delayMs;

  private long runThreadId = -1L;
  private final List<Long> queueTimes = new ArrayList<>();
  private final List<Long> sleepTimes = new ArrayList<>();

  public MockEventSender(Dashdive dashdive, int eventCount, int delayMs) {
    this.interceptor = dashdive.getInterceptorForImperativeTrigger();
    this.eventCount = eventCount;
    this.delayMs = delayMs;
  }

  private String formatTime(double time) {
    return String.format("%4.2f", ((double) time) / 1_000_000);
  }

  public String getPostRunStatsPrintOut() {
    final int FIRST_N = 10;
    final int REMAINING_N = eventCount - FIRST_N;

    final long queueTimeTotalFirst =
        queueTimes.stream().limit(FIRST_N).mapToLong(Long::longValue).sum();
    final long queueTimeTotalRemaining =
        queueTimes.stream().skip(FIRST_N).mapToLong(Long::longValue).sum();

    final long sleepTimeTotalFirst =
        sleepTimes.stream().limit(FIRST_N).mapToLong(Long::longValue).sum();
    final long sleepTimeTotalRemaining =
        sleepTimes.stream().skip(FIRST_N).mapToLong(Long::longValue).sum();

    final StringBuilder builder = new StringBuilder();
    builder.append("Thread ID: " + runThreadId + "\n");
    builder.append("=== FIRST " + FIRST_N + " EVENTS ===" + "\n");
    builder.append(
        "Queue time average (ms): " + formatTime(((double) queueTimeTotalFirst) / FIRST_N) + "\n");
    builder.append(
        "Sleep time average (ms): " + formatTime(((double) sleepTimeTotalFirst) / FIRST_N) + "\n");
    builder.append(
        "Queue time as percentage: "
            + String.format(
                "%2.1f",
                ((double) queueTimeTotalFirst) / (queueTimeTotalFirst + sleepTimeTotalFirst) * 100)
            + "%"
            + "\n");
    builder.append("=== REMAINING " + REMAINING_N + " EVENTS ===" + "\n");
    builder.append(
        "Queue time average (ms): "
            + formatTime(((double) queueTimeTotalRemaining) / REMAINING_N)
            + "\n");
    builder.append(
        "Sleep time average (ms): "
            + formatTime(((double) sleepTimeTotalRemaining) / REMAINING_N)
            + "\n");
    builder.append(
        "Queue time as percentage: "
            + String.format(
                "%2.1f",
                ((double) queueTimeTotalRemaining)
                    / (queueTimeTotalRemaining + sleepTimeTotalRemaining)
                    * 100)
            + "%"
            + "\n");
    builder.append("\n");

    return builder.toString();
  }

  @Override
  public void run() {
    if (runThreadId != -1L) {
      throw new IllegalStateException("Thread already run");
    }

    long threadId = Thread.currentThread().threadId();
    runThreadId = threadId;
    for (int eventIndex = 0; eventIndex < eventCount; eventIndex++) {
      String eventId = Long.toString(threadId) + "-" + Integer.toString(eventIndex);
      InterceptorContext context = TestUtils.getListObjectsV2Event(eventId);

      try {
        final long startSleepTime = System.nanoTime();
        // Simulate actual activity by the S3Client, e.g. network calls
        Thread.sleep(delayMs);
        final long endSleepTime = System.nanoTime();
        sleepTimes.add(endSleepTime - startSleepTime);
      } catch (InterruptedException ignored) {
      }

      final long startQueueTime = System.nanoTime();
      interceptor.afterExecution(context, TestUtils.EXEC_ATTRS_EMPTY);
      final long endQueueTime = System.nanoTime();
      queueTimes.add(endQueueTime - startQueueTime);
    }
  }
}

/*
=== MOTIVATION ===

The goal of this test is twofold:
  1. Test the correctness of the batching logic across the entire pipeline
     when many threads are sending events in parallel.
  2. Test the performance of individual event ingestion under those same conditions.

Correctness entails the following:
  - When batches accumulate in the queue due to slow processing relative to enqueueing,
    the queue should drop events while full. This should be reflected in logs and metrics.
  - The pipeline should be robust to high parallelism and throughput, and all events
    should be accounted for in some form.
  - All metrics should be consistent with each other, as well as with the total
    number of events enqueued and the total number of batches sent.

Performance entails that the resources required to track each event are minimal
relative to the actual cloud events (e.g. S3Client invocations) themselves.
In particular, every event tracked and queued should incur minimal latency,
and as a total percentage of compute time, should also be minimal. Checking
these conditions is the purpose of the `getPostRunStats()` "benchmark".


=== IMPLEMENTATION ===

We achieve high throughput and guarantee a full queue as follows:
- Start 4 threads, each sending events roughly every 1ms
- Set per-thread batch size to be 5 events
- Simulate 100ms of latency for each HTTP batch ingestion by having
  `MockHttpClient` sleep for 100ms on each invocation

Our maximum queue size is 100 batches (BatchEventProcessor.java: `EXECUTOR_QUEUE_MAX_BATCHES = 100`).
Every 100ms, the total number of generated batches across all 4 threads is 4 * 100 / 5 = 80 batches.
We can have a maximum of 10 threads (BatchEventProcessor.java: `EXECUTOR_MAX_POOL_SIZE = 10`).
"sending" these batches to the ingest service, each with a 100 ms delay, so the processor can get rid
of 10 batches every 100ms. This means a net accumulation of roughly 70 batches every 100ms.
If we repeat this for 250ms, this should be a net accumulation of 175 batches, which virtually guarantees
a full queue and dropped events (safety factor 175/101 = 1.73).
*/

public class ParallelPerfTest {
  @Test
  void testBatcherBehavior() {
    final MockHttpClient batchesMockedClient =
        new MockHttpClient(
            () -> {
              try {
                Thread.sleep(100);
              } catch (InterruptedException exception) {
              }
            });
    final MockHttpClient incrementalMetricsMockedClient = new MockHttpClient();
    final MockHttpClient totalMetricsMockedClient = new MockHttpClient();
    final MockHttpClient ignoredMockedClient = new MockHttpClient();

    final int BATCH_SIZE = 5;
    final String INSTANCE_ID = "parallel-perf";

    final S3EventAttributeExtractorFactory factoryWithCustomerId =
        S3EventAttributeExtractorFactory.from(
            (input) -> ImmutableS3EventAttributes.builder().customerId("dummy-customer").build());

    final SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder().classInstanceId(INSTANCE_ID).build())
            .targetEventBatchSize(BATCH_SIZE)
            .startupTelemetryWarnings(TelemetryPayload.of())
            .build();
    final Dashdive dashdive =
        new Dashdive(
            TestUtils.API_KEY_DUMMY,
            Optional.of(factoryWithCustomerId),
            Optional.empty(),
            totalMetricsMockedClient.getDelegate(),
            ignoredMockedClient.getDelegate(),
            batchesMockedClient.getDelegate(),
            incrementalMetricsMockedClient.getDelegate(),
            Optional.of(setupDefaults));
    dashdive.blockUntilSetupComplete();

    final int THREAD_COUNT = 4;
    final int EVENTS_PER_THREAD = 250;
    final int DELAY_MS = 1;

    final List<MockEventSender> eventSenders =
        Stream.generate(() -> new MockEventSender(dashdive, EVENTS_PER_THREAD, DELAY_MS))
            .limit(THREAD_COUNT)
            .collect(Collectors.toList());

    final List<Thread> workerThreads =
        eventSenders.stream().map(sender -> new Thread(sender)).collect(Collectors.toList());
    workerThreads.forEach(Thread::start);

    workerThreads.forEach(
        (thread) -> {
          try {
            thread.join();
          } catch (InterruptedException exception) {
          }
        });

    eventSenders.stream().map(MockEventSender::getPostRunStatsPrintOut).forEach(System.out::print);

    dashdive.close();
    dashdive.blockUntilShutdownComplete();

    final List<String> incrementalMetricsReqBodies =
        incrementalMetricsMockedClient.unboxRequestBodiesAssertingNonempty();
    final List<Map<String, Integer>> incrementalMetrics =
        incrementalMetricsReqBodies.stream()
            .map(
                metricReqBody ->
                    MetricsUtils.getMetricsFromRequestBodyWithAssertions(
                        MetricsUtils.Type.INCREMENTAL, metricReqBody, INSTANCE_ID))
            .collect(Collectors.toList());
    final Map<String, Integer> combinedIncrementalMetrics =
        MetricsUtils.getCombinedMetricsFromIncremental(incrementalMetrics);

    Assertions.assertEquals(1, totalMetricsMockedClient.getRequests().size());
    final String totalMetricsReqBody =
        totalMetricsMockedClient.unboxRequestBodiesAssertingNonempty().getFirst();
    final Map<String, Integer> totalMetrics =
        MetricsUtils.getMetricsFromRequestBodyWithAssertions(
            MetricsUtils.Type.TOTAL, totalMetricsReqBody, INSTANCE_ID);

    Assertions.assertEquals(totalMetrics, combinedIncrementalMetrics);

    // Some events should have been sent successfully; others should have been dropped from a full
    // queue
    Assertions.assertTrue(
        totalMetrics.get(EventPipelineMetrics.Type.EVENTS_DROPPED_FROM_QUEUE.toWireString()) > 0);
    Assertions.assertTrue(
        totalMetrics.get(EventPipelineMetrics.Type.EVENTS_SENT.toWireString()) > 0);

    final int TOTAL_EVENTS = THREAD_COUNT * EVENTS_PER_THREAD;
    final int TOTAL_BATCHES = TOTAL_EVENTS / BATCH_SIZE;

    // All events should have either been dropped from the queue or enqueued (exactly once)
    Assertions.assertEquals(
        TOTAL_EVENTS,
        totalMetrics.get(EventPipelineMetrics.Type.EVENTS_ENQUEUED.toWireString())
            + totalMetrics.get(EventPipelineMetrics.Type.EVENTS_DROPPED_FROM_QUEUE.toWireString()));
    Assertions.assertEquals(
        TOTAL_BATCHES,
        totalMetrics.get(EventPipelineMetrics.Type.BATCHES_ENQUEUED.toWireString())
            + totalMetrics.get(
                EventPipelineMetrics.Type.BATCHES_DROPPED_FROM_QUEUE.toWireString()));

    // Number enqueued should equal number sent (none dropped once enqueued)
    Assertions.assertEquals(
        totalMetrics.get(EventPipelineMetrics.Type.EVENTS_ENQUEUED.toWireString()),
        totalMetrics.get(EventPipelineMetrics.Type.EVENTS_SENT.toWireString()));
    Assertions.assertEquals(
        totalMetrics.get(EventPipelineMetrics.Type.BATCHES_ENQUEUED.toWireString()),
        totalMetrics.get(EventPipelineMetrics.Type.BATCHES_SENT.toWireString()));

    // All batches should have been entirely full, and event metrics should track batch metrics 1:1
    Assertions.assertEquals(
        totalMetrics.get(EventPipelineMetrics.Type.BATCHES_ENQUEUED.toWireString()),
        totalMetrics.get(EventPipelineMetrics.Type.EVENTS_ENQUEUED.toWireString()) / BATCH_SIZE);
    Assertions.assertEquals(
        totalMetrics.get(EventPipelineMetrics.Type.BATCHES_SENT.toWireString()),
        totalMetrics.get(EventPipelineMetrics.Type.EVENTS_SENT.toWireString()) / BATCH_SIZE);
    Assertions.assertEquals(
        totalMetrics.get(EventPipelineMetrics.Type.BATCHES_DROPPED_FROM_QUEUE.toWireString()),
        totalMetrics.get(EventPipelineMetrics.Type.EVENTS_DROPPED_FROM_QUEUE.toWireString())
            / BATCH_SIZE);

    // Test that events sent and batches sent correspond to what is
    // seen in the "batch ingest" HTTP client
    final List<String> batchesReqBodies = batchesMockedClient.unboxRequestBodiesAssertingNonempty();
    final List<Map<String, Object>> ingestedEvents =
        TestUtils.getIngestedEventsFromRequestBodies(batchesReqBodies);
    Assertions.assertEquals(
        totalMetrics.get(EventPipelineMetrics.Type.BATCHES_SENT.toWireString()),
        batchesReqBodies.size());
    Assertions.assertEquals(
        totalMetrics.get(EventPipelineMetrics.Type.EVENTS_SENT.toWireString()),
        ingestedEvents.size());

    // All other metrics than those we're looking for should be zero
    final Set<String> expectedNonzeroMetricTypes =
        Set.of(
            EventPipelineMetrics.Type.EVENTS_DROPPED_FROM_QUEUE.toWireString(),
            EventPipelineMetrics.Type.EVENTS_ENQUEUED.toWireString(),
            EventPipelineMetrics.Type.EVENTS_SENT.toWireString(),
            EventPipelineMetrics.Type.BATCHES_DROPPED_FROM_QUEUE.toWireString(),
            EventPipelineMetrics.Type.BATCHES_ENQUEUED.toWireString(),
            EventPipelineMetrics.Type.BATCHES_SENT.toWireString());

    Assertions.assertTrue(
        totalMetrics.entrySet().stream()
            .filter(entry -> !expectedNonzeroMetricTypes.contains(entry.getKey()))
            .allMatch(entry -> entry.getValue() == 0));
  }
}
