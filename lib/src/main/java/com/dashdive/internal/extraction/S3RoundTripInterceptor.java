package com.dashdive.internal.extraction;

import com.dashdive.internal.ImmutableS3SingleExtractedEvent;
import com.dashdive.internal.S3SingleExtractedEvent;
import com.dashdive.internal.batching.SingleEventBatcher;
import com.dashdive.internal.telemetry.ExceptionUtil;
import com.dashdive.internal.telemetry.ImmutableTelemetryItem;
import com.dashdive.internal.telemetry.TelemetryPayload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;

public class S3RoundTripInterceptor implements ExecutionInterceptor {
  private static final Logger logger = LoggerFactory.getLogger(S3RoundTripInterceptor.class);

  private final SingleEventBatcher batcher;

  public S3RoundTripInterceptor(SingleEventBatcher batcher) {
    this.batcher = batcher;
  }

  /*
  =================================================
  POTENTIAL ALTERNATIVE DATA EXTRACTION APPROACHES
  =================================================

  // May be able to extract region in the following manner, but may not always correspond to the region of the S3 bucket:
  final Optional<Region> regionAttr =
    getExecutionAttributeValueByKeyName(executionAttributes, "AwsRegion")
        .map(res -> (Region) res);
  private Optional<Object> getExecutionAttributeValueByKeyName(
      ExecutionAttributes executionAttributes, String keyName) {
    return executionAttributes.getAttributes().entrySet().stream()
        .filter((entry) -> entry.getKey().toString().equals(keyName))
        .findFirst()
        .map((entry) -> entry.getValue());
  }
  // Could also use a similar method as above to extract OperationName (e.g. ListObjectsV2).

  // Other interesting fields include: ServiceConfig (s3.S3Configuration), ResolvedEndpoint (endpoints.Endpoint)
  */

  private static final String sentPayloadKey = "dashdive.hasSentPayload";
  private static final ExecutionAttribute<Boolean> sentPayloadAttr =
      new ExecutionAttribute<>(sentPayloadKey);

  // There are 4 cases where `onExecutionFailure` could be triggered.
  // The request could have been successful or not,
  // and `afterExecution` could've already run or not.
  // Case 1 (successful/before): Send payload AND telemetry; tell `afterExecution` to NOT send (by
  // setting flag)
  // Case 2 (successful/after): Send only telemetry
  // Case 3 (failed/before): Send only telemetry
  // Case 4 (failed/after): Send only telemetry

  @Override
  public void onExecutionFailure(
      Context.FailedExecution context, ExecutionAttributes executionAttributes) {
    // We correlate the successful enqueueing of each event across `onExecutionFailure`
    // and `afterExecution` to ensure it's only done once per event.
    try {
      final boolean hasSentPayload =
          executionAttributes.getOptionalAttribute(sentPayloadAttr).orElse(false);

      final TelemetryPayload executionFailurePayload =
          TelemetryPayload.from(
              ImmutableTelemetryItem.builder()
                  .type("ON_EXECUTION_FAILURE")
                  .data(
                      ImmutableMap.of(
                          "exception",
                          ExceptionUtil.getSerializableExceptionData(context.exception())))
                  .build());

      // As documented in `isSuccessful`, request was successful
      // if and only if response status code is 2xx
      final boolean wasRequestSuccessful =
          context.httpRequest().isPresent()
              && context.response().isPresent()
              && context.httpResponse().isPresent()
              && context.httpResponse().get().isSuccessful();
      // If request was successful, extract payload as normal but also send telemetry.
      // If request failed, only send telemetry.
      if (wasRequestSuccessful && !hasSentPayload) {
        final S3SingleExtractedEvent extractedPayload =
            S3RoundTripExtractor.extractEventPayloadSafely(
                ImmutableS3RoundTripData.builder()
                    .pojoRequest(context.request())
                    .pojoResponse(context.response().get())
                    // TODO: There doesn't seem to be a way to get `requestBody` from `context`?
                    .httpRequest(context.httpRequest().get())
                    .httpResponse(context.httpResponse().get())
                    .build());

        final S3SingleExtractedEvent extractedPayloadWithTelemetry =
            ImmutableS3SingleExtractedEvent.builder()
                .from(extractedPayload)
                .telemetryWarnings(executionFailurePayload)
                .build();
        this.batcher.queueEventForIngestion(extractedPayloadWithTelemetry);

        executionAttributes.putAttribute(sentPayloadAttr, true);
      } else {
        final S3SingleExtractedEvent failedEventPayload =
            ImmutableS3SingleExtractedEvent.builder()
                .hasIrrecoverableErrors(true)
                .telemetryErrors(executionFailurePayload)
                .build();
        this.batcher.queueEventForIngestion(failedEventPayload);
      }
    } catch (Exception exception) {
      logger.error(
          "Failed to extract or enqueue S3 event payload in `onExecutionFailure`", exception);
    }
  }

  private boolean safeAfterExecutionReturningSuccess(
      Context.AfterExecution context, ExecutionAttributes executionAttributes) {
    try {
      final boolean hasSentPayload =
          executionAttributes.getOptionalAttribute(sentPayloadAttr).orElse(false);
      if (hasSentPayload) {
        return true;
      }

      final S3SingleExtractedEvent extractedPayload =
          S3RoundTripExtractor.extractEventPayloadSafely(
              ImmutableS3RoundTripData.builder()
                  .pojoRequest(context.request())
                  .pojoResponse(context.response())
                  .requestBody(context.requestBody())
                  .httpRequest(context.httpRequest())
                  .httpResponse(context.httpResponse())
                  .build());
      return this.batcher.queueEventForIngestion(extractedPayload);
    } catch (Exception exception) {
      logger.error("Failed to extract or enqueue S3 event payload in `afterExecution`", exception);
      return false;
    }
  }

  @Override
  public void afterExecution(
      Context.AfterExecution context, ExecutionAttributes executionAttributes) {
    safeAfterExecutionReturningSuccess(context, executionAttributes);
  }

  @VisibleForTesting
  public boolean _afterExecutionReturningSuccess(
      Context.AfterExecution context, ExecutionAttributes executionAttributes) {
    return safeAfterExecutionReturningSuccess(context, executionAttributes);
  }
}
