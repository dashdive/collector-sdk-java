package com.dashdive;

import com.dashdive.internal.ImmutableDashdiveInstanceInfo;
import com.dashdive.internal.ImmutableSetupDefaults;
import com.dashdive.internal.SetupDefaults;
import com.dashdive.internal.extraction.S3RoundTripInterceptor;
import com.dashdive.internal.telemetry.TelemetryPayload;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ServiceIdTest {
  @Test
  void testServiceIdParser() {
    List<Pair<String, String>> testCases =
        Arrays.asList(
            Pair.of("", ""),
            Pair.of("com.dashdive.DashdiveImpl", "com.dashdive"),
            Pair.of(".....", "....."),
            Pair.of("main", "main"),
            Pair.of("MyMainClass", "MyMainClass"),
            Pair.of("main.MyMainClass", "main.MyMainClass"),
            Pair.of("mycompanyservice.main.MyMainClass", "mycompanyservice"),
            Pair.of("com.rhombus.cloud.rs3.main.Main", "com.rhombus.cloud.rs3"),
            Pair.of(
                "com.rhombus.cloud.sqsconsumer.pose.service.main.SqsConsumerPoseApplication",
                "com.rhombus.cloud.sqsconsumer.pose.service"),
            Pair.of(
                "com.dashdive.some_svc.MainClass -Dsome.key=some.value --some_arg vals",
                "com.dashdive.some_svc"),
            Pair.of(
                "rhombus-cloud-sqsconsumer-graphics-service-1.0.18-17-g7efbbaa.jar   ",
                "rhombus-cloud-sqsconsumer-graphics-service"));

    for (Pair<String, String> testCase : testCases) {
      String input = testCase.getLeft();
      String expected = testCase.getRight();
      String actual = DashdiveImpl.getServiceIdFromStartCommand(input).orElse("");
      Assertions.assertEquals(expected, actual);
    }
  }

  @Test
  void testServiceIdEndToEndPropagation() {
    final String mainClassPath =
        "com.rhombus.cloud.sqsconsumer.pose.service.main.SqsConsumerPoseApplication";
    final String expectedServiceId = "com.rhombus.cloud.sqsconsumer.pose.service";
    System.setProperty(DashdiveImpl.SERVICE_ID_TEST_SYSTEM_PROPERTY_KEY, mainClassPath);

    final MockHttpClient ignoredMockHttpClient = new MockHttpClient();
    final MockHttpClient batchMockHttpClient = new MockHttpClient();

    final SetupDefaults setupDefaults =
        ImmutableSetupDefaults.builder()
            .dashdiveInstanceInfo(
                ImmutableDashdiveInstanceInfo.builder().classInstanceId("service-id-test").build())
            .targetEventBatchSize(1)
            .startupTelemetryWarnings(TelemetryPayload.of())
            .build();
    final DashdiveImpl dashdive =
        new DashdiveImpl(
            Dashdive.DEFAULT_INGEST_BASE_URI,
            TestUtils.API_KEY_DUMMY,
            Optional.of(TestUtils.EXTRACTOR_CUSTOMER),
            Optional.empty(),
            ignoredMockHttpClient.getDelegate(),
            ignoredMockHttpClient.getDelegate(),
            batchMockHttpClient.getDelegate(),
            ignoredMockHttpClient.getDelegate(),
            Optional.of(setupDefaults));

    final S3RoundTripInterceptor interceptor = dashdive.getInterceptorForImperativeTrigger();
    interceptor.afterExecution(
        TestUtils.getListObjectsV2Event("test-bucket-0"), TestUtils.EXEC_ATTRS_EMPTY);

    dashdive.close();
    dashdive.blockUntilShutdownComplete();

    final List<Map<String, Object>> ingestedEvents =
        TestUtils.getIngestedEventsFromRequestBodies(
            batchMockHttpClient.unboxRequestBodiesAssertingNonempty());

    Assertions.assertEquals(1, ingestedEvents.size());
    Assertions.assertEquals(expectedServiceId, ingestedEvents.get(0).get("serviceId"));
  }
}
