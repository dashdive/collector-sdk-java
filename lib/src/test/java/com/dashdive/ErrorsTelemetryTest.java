package com.dashdive;

import com.dashdive.MockHttpClient.HttpRequestWithBody;
import com.dashdive.internal.DashdiveConnection;
import com.dashdive.internal.ImmutableDashdiveInstanceInfo;
import com.dashdive.internal.ImmutableSetupDefaults;
import com.dashdive.internal.SetupDefaults;
import com.dashdive.internal.telemetry.TelemetryPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;

class ErrorsTelemetryTest {
  @SuppressWarnings("unchecked")
  @Test
  void testS3Client4xxError() {
    final MockHttpClient sharedMockedClient = new MockHttpClient();

    final SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder().classInstanceId("test-client-4xx").build())
            .targetEventBatchSize(100)
            .startupTelemetryWarnings(TelemetryPayload.of())
            .build();
    final Dashdive dashdive =
        new Dashdive(
            TestUtils.API_KEY_DUMMY,
            Optional.of(TestUtils.FACTORY_EMPTY),
            Optional.empty(),
            sharedMockedClient.getDelegate(),
            sharedMockedClient.getDelegate(),
            sharedMockedClient.getDelegate(),
            sharedMockedClient.getDelegate(),
            Optional.of(setupDefaults));

    final S3Client s3Client =
        dashdive.withInstrumentation(S3Client.builder().region(Region.EU_CENTRAL_1)).build();

    final String uuid = "bac79f5b-a302-4ad9-af79-6bf96f9446be";
    final HeadBucketRequest headBucketRequest =
        HeadBucketRequest.builder().bucket("NONEXISTENT-BUCKET-" + uuid).build();

    Exception encountered = null;
    try {
      s3Client.headBucket(headBucketRequest);
    } catch (NoSuchBucketException exception) {
      encountered = exception;
    }

    Assertions.assertTrue(encountered.getClass().getName().endsWith("NoSuchBucketException"));

    s3Client.close();
    dashdive.close();

    final List<HttpRequestWithBody> matchingRequests =
        sharedMockedClient.getRequests().stream()
            .filter(
                req ->
                    req.request()
                        .uri()
                        .getPath()
                        .equals(
                            DashdiveConnection.getRoute(
                                    DashdiveConnection.Route.TELEMETRY_EXTRACTION_ISSUES)
                                .getPath()))
            .collect(Collectors.toList());
    final List<String> issueEventStrings =
        MockHttpClient.unboxRequestBodiesAssertingNonempty(matchingRequests);

    final ObjectMapper objectMapper = new ObjectMapper();
    final TypeReference<Map<String, Object>> typeReference =
        new TypeReference<Map<String, Object>>() {};
    final List<Map<String, Object>> issueEventObjects =
        issueEventStrings.stream()
            .map(
                body -> {
                  try {
                    final Map<String, Object> singleEvent =
                        objectMapper.readValue(body, typeReference);
                    return singleEvent;
                  } catch (JsonProcessingException exception) {
                    Assertions.fail("Failed to parse JSON request body: " + exception.getMessage());
                    return null;
                  }
                })
            .collect(Collectors.toList());

    Assertions.assertEquals(1, issueEventObjects.size());
    final List<Map<String, Object>> eventsWithIssues =
        (List<Map<String, Object>>) issueEventObjects.getFirst().get("eventsWithIssues");

    Assertions.assertEquals(1, eventsWithIssues.size());
    final Map<String, Object> eventWithIssues = eventsWithIssues.getFirst();

    Assertions.assertEquals(true, eventWithIssues.get("hasIrrecoverableErrors"));
    Assertions.assertEquals(0, ((List<Object>) eventWithIssues.get("telemetryWarnings")).size());

    final List<Map<String, Object>> telemetryErrors =
        (List<Map<String, Object>>) eventWithIssues.get("telemetryErrors");
    Assertions.assertEquals(2, telemetryErrors.size());
    Assertions.assertEquals("INFER_REQUIRED_FIELD_FAILURE", telemetryErrors.get(0).get("type"));
    Assertions.assertEquals("ON_EXECUTION_FAILURE", telemetryErrors.get(1).get("type"));
  }
}
