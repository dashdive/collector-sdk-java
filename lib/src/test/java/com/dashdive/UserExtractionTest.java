package com.dashdive;

import com.dashdive.internal.ConnectionUtils;
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

public class UserExtractionTest {
  private static void assertWarningInAllExtractionIssues(
      List<String> extractionIssuesRequestBodies, int expectedEventsWithIssuesCount) {

    final List<Map<String, Object>> extractionIssuesList =
        TestUtils.getObjectsFromRequestBodies(extractionIssuesRequestBodies);

    // Ensure that the body sent to the "extraction issues" endpoint for the only batch
    // contains the expected warnings and no others

    int totalEventsWithIssuesCount = 0;
    for (final Map<String, Object> extractionIssues : extractionIssuesList) {

      Assertions.assertInstanceOf(List.class, extractionIssues.get("eventsWithIssues"));
      @SuppressWarnings("unchecked")
      final List<Map<String, Object>> eventsWithIssues =
          (List<Map<String, Object>>) extractionIssues.get("eventsWithIssues");
      totalEventsWithIssuesCount += eventsWithIssues.size();

      for (final Map<String, Object> eventWithIssue : eventsWithIssues) {
        Assertions.assertInstanceOf(List.class, eventWithIssue.get("telemetryWarnings"));
        @SuppressWarnings("unchecked")
        final List<Map<String, Object>> warnings =
            (List<Map<String, Object>>) eventWithIssue.get("telemetryWarnings");

        Assertions.assertTrue(
            warnings.stream().anyMatch(warning -> warning.get("type").equals("EMPTY_USER_ATTRS")));
      }
    }

    Assertions.assertEquals(expectedEventsWithIssuesCount, totalEventsWithIssuesCount);
  }

  @Test
  void emptyFactoryIsIgnoredWithWarnings() {
    final MockHttpClient ignoredMockHttpClient = new MockHttpClient();
    final MockHttpClient batchMockHttpClient = new MockHttpClient();

    final int BATCH_SIZE = 2;

    final SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder()
                    .classInstanceId("extraction-empty-factory")
                    .build())
            .targetEventBatchSize(BATCH_SIZE)
            .startupTelemetryWarnings(TelemetryPayload.of())
            .build();
    final DashdiveImpl dashdive =
        new DashdiveImpl(
            Dashdive.DEFAULT_INGEST_BASE_URI,
            TestUtils.API_KEY_DUMMY,
            Optional.empty(),
            Optional.empty(),
            ignoredMockHttpClient.getDelegate(),
            ignoredMockHttpClient.getDelegate(),
            batchMockHttpClient.getDelegate(),
            ignoredMockHttpClient.getDelegate(),
            Optional.of(setupDefaults));

    final S3RoundTripInterceptor interceptor = dashdive.getInterceptorForImperativeTrigger();
    for (int i = 0; i < BATCH_SIZE; i++) {
      interceptor.afterExecution(TestUtils.GENERIC_INTERCEPTED_EVENT, TestUtils.EXEC_ATTRS_EMPTY);
    }

    dashdive.close();
    dashdive.blockUntilShutdownComplete();

    final String extractionIssuesPath =
        ConnectionUtils.getFullUri(
            Dashdive.DEFAULT_INGEST_BASE_URI, ConnectionUtils.Route.TELEMETRY_EXTRACTION_ISSUES).getPath();
    final List<Optional<String>> extractionIssuesReqBodies =
        batchMockHttpClient.getRequests().stream()
            .filter(req -> extractionIssuesPath.equals(req.request().uri().getPath()))
            .map(req -> req.body())
            .collect(Collectors.toList());

    Assertions.assertEquals(1, extractionIssuesReqBodies.size());
    Assertions.assertTrue(extractionIssuesReqBodies.get(0).isPresent());

    assertWarningInAllExtractionIssues(List.of(extractionIssuesReqBodies.get(0).get()), BATCH_SIZE);
  }

  @Test
  void nullExtractionIsIgnoredWithWarnings() {
    final S3EventAttributeExtractor factoryReturningNull = (input) -> null;

    final MockHttpClient ignoredMockHttpClient = new MockHttpClient();
    final MockHttpClient batchMockHttpClient = new MockHttpClient();

    final int BATCH_SIZE = 2;

    final SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder()
                    .classInstanceId("extraction-null-return")
                    .build())
            .targetEventBatchSize(BATCH_SIZE)
            .startupTelemetryWarnings(TelemetryPayload.of())
            .build();
    final DashdiveImpl dashdive =
        new DashdiveImpl(
            Dashdive.DEFAULT_INGEST_BASE_URI,
            TestUtils.API_KEY_DUMMY,
            Optional.of(factoryReturningNull),
            Optional.empty(),
            ignoredMockHttpClient.getDelegate(),
            ignoredMockHttpClient.getDelegate(),
            batchMockHttpClient.getDelegate(),
            ignoredMockHttpClient.getDelegate(),
            Optional.of(setupDefaults));

    final S3RoundTripInterceptor interceptor = dashdive.getInterceptorForImperativeTrigger();
    for (int i = 0; i < BATCH_SIZE; i++) {
      interceptor.afterExecution(TestUtils.GENERIC_INTERCEPTED_EVENT, TestUtils.EXEC_ATTRS_EMPTY);
    }

    dashdive.close();
    dashdive.blockUntilShutdownComplete();

    final String extractionIssuesPath =
        ConnectionUtils.getFullUri(
            Dashdive.DEFAULT_INGEST_BASE_URI, ConnectionUtils.Route.TELEMETRY_EXTRACTION_ISSUES).getPath();
    final List<Optional<String>> extractionIssuesReqBodies =
        batchMockHttpClient.getRequests().stream()
            .filter(req -> extractionIssuesPath.equals(req.request().uri().getPath()))
            .map(req -> req.body())
            .collect(Collectors.toList());

    Assertions.assertEquals(1, extractionIssuesReqBodies.size());
    Assertions.assertTrue(extractionIssuesReqBodies.get(0).isPresent());

    assertWarningInAllExtractionIssues(List.of(extractionIssuesReqBodies.get(0).get()), BATCH_SIZE);
  }

  @Test
  void userExtractorWorksAsExpected() {
    final S3EventAttributeExtractor factoryWithFeatureId =
        (input) -> {
          final String featureId =
              input
                  .bucketName()
                  .map(
                      bucketName -> {
                        final List<String> parts = List.of(bucketName.split("-"));
                        final String numberId = parts.get(parts.size() - 1);
                        return "feature-" + numberId;
                      })
                  .orElse("UNKNOWN");
          return ImmutableS3EventAttributes.builder().featureId(featureId).build();
        };

    final MockHttpClient ignoredMockHttpClient = new MockHttpClient();
    final MockHttpClient batchMockHttpClient = new MockHttpClient();

    final int BATCH_SIZE = 2;
    final int BATCH_COUNT = 3;
    final int TOTAL_EVENTS = BATCH_SIZE * BATCH_COUNT;

    final SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder().classInstanceId("user-extractor").build())
            .targetEventBatchSize(BATCH_SIZE)
            .startupTelemetryWarnings(TelemetryPayload.of())
            .build();
    final DashdiveImpl dashdive =
        new DashdiveImpl(
            Dashdive.DEFAULT_INGEST_BASE_URI,
            TestUtils.API_KEY_DUMMY,
            Optional.of(factoryWithFeatureId),
            Optional.empty(),
            ignoredMockHttpClient.getDelegate(),
            ignoredMockHttpClient.getDelegate(),
            batchMockHttpClient.getDelegate(),
            ignoredMockHttpClient.getDelegate(),
            Optional.of(setupDefaults));

    final S3RoundTripInterceptor interceptor = dashdive.getInterceptorForImperativeTrigger();
    for (int i = 0; i < TOTAL_EVENTS; i++) {
      interceptor.afterExecution(
          TestUtils.getListObjectsV2Event("test-bucket-" + i), TestUtils.EXEC_ATTRS_EMPTY);
    }

    dashdive.close();
    dashdive.blockUntilShutdownComplete();

    Assertions.assertEquals(BATCH_COUNT, batchMockHttpClient.getRequests().size());

    final List<String> batchIngestBodies =
        batchMockHttpClient.unboxRequestBodiesAssertingNonempty();
    batchMockHttpClient.assertAllUrisMatch(
        ConnectionUtils.getFullUri(
            Dashdive.DEFAULT_INGEST_BASE_URI, ConnectionUtils.Route.S3_BATCH_INGEST).getPath());
    final List<Map<String, Object>> ingestedEvents =
        TestUtils.getIngestedEventsFromRequestBodies(batchIngestBodies);

    Assertions.assertEquals(TOTAL_EVENTS, ingestedEvents.size());
    ingestedEvents.sort(
        (a, b) -> ((String) a.get("featureId")).compareTo((String) b.get("featureId")));
    for (int i = 0; i < TOTAL_EVENTS; i++) {
      final Map<String, Object> event = ingestedEvents.get(i);
      Assertions.assertEquals("feature-" + i, event.get("featureId"));
      Assertions.assertEquals("test-bucket-" + i, event.get("bucket"));
    }
  }
}
