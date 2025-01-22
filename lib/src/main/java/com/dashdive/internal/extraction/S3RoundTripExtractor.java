package com.dashdive.internal.extraction;

import com.dashdive.S3Provider;
import com.dashdive.internal.ImmutableS3SingleExtractedEvent;
import com.dashdive.internal.S3EventFieldName;
import com.dashdive.internal.S3SingleExtractedEvent;
import com.dashdive.internal.telemetry.ExceptionUtil;
import com.dashdive.internal.telemetry.ImmutableTelemetryItem;
import com.dashdive.internal.telemetry.TelemetryPayload;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.time.DateTimeException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

public class S3RoundTripExtractor {

  private static Optional<String> toDateTimeISO8601(String dateTimeRFC1121) {
    try {
      ZonedDateTime zoned =
          ZonedDateTime.parse(dateTimeRFC1121, DateTimeFormatter.RFC_1123_DATE_TIME);
      return Optional.of(DateTimeFormatter.ISO_INSTANT.format(zoned));
    } catch (DateTimeException parseOrFormatException) {
      return Optional.empty();
    }
  }

  @VisibleForTesting public static final String S3_RESPONSE_DATE_HEADER = "date";

  // `additinoalPayloadFields` CANNOT contain any null values
  public static S3SingleExtractedEvent extractEventPayloadSafely(
      S3RoundTripData roundTripData, Map<String, Object> additionalPayloadFields) {
    try {
      return extractEventPayload(roundTripData, additionalPayloadFields);
    } catch (Exception exception) {
      return ImmutableS3SingleExtractedEvent.builder()
          .hasIrrecoverableErrors(true)
          .telemetryErrors(
              TelemetryPayload.from(
                  ImmutableTelemetryItem.builder()
                      .type("uncaughtExtractionException")
                      .data(
                          ImmutableMap.of(
                              "inputData",
                              roundTripData,
                              "additionalPayloadFields",
                              additionalPayloadFields,
                              "exception",
                              ExceptionUtil.getSerializableExceptionData(exception)))
                      .build()))
          .build();
    }
  }

  private static S3SingleExtractedEvent extractEventPayload(
      S3RoundTripData roundTripData, Map<String, Object> additionalPayloadFields) {
    final TelemetryPayload.Builder telemetryWarningsBuilder = TelemetryPayload.builder();

    // Extract timestamp

    final Optional<String> eventDateTimeHeader =
        roundTripData.httpResponse().firstMatchingHeader(S3_RESPONSE_DATE_HEADER);
    final Optional<String> maybeEventDateTimeISO8601 =
        eventDateTimeHeader.flatMap(S3RoundTripExtractor::toDateTimeISO8601);

    String eventDateTimeISO8601;
    if (maybeEventDateTimeISO8601.isPresent()) {
      eventDateTimeISO8601 = maybeEventDateTimeISO8601.get();
    } else {
      eventDateTimeISO8601 =
          ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT);
      telemetryWarningsBuilder.add(
          ImmutableTelemetryItem.builder()
              .type("defaultDateWarning")
              .data(
                  ImmutableMap.of(
                      "message",
                      "Failed to parse date header from S3 response",
                      "inputValue",
                      eventDateTimeHeader.orElse("")))
              .build());
    }

    // Extract S3 provider

    final String requestHost = roundTripData.httpRequest().host();
    final Optional<S3Provider> inferredS3Provider = S3Provider.inferFrom(requestHost);
    S3Provider s3Provider;
    if (inferredS3Provider.isPresent()) {
      s3Provider = inferredS3Provider.get();
    } else {
      s3Provider = S3Provider.AWS;
      telemetryWarningsBuilder.add(
          ImmutableTelemetryItem.builder()
              .type("defaultProviderWarning")
              .data(
                  ImmutableMap.of(
                      "message",
                      "Failed to infer S3 provider from request",
                      "inputValue",
                      requestHost))
              .build());
    }

    // Extract event-specific fields

    final S3DistinctEventData distinctData;
    try {
      distinctData = S3DistinctEventDataExtractor.getFrom(roundTripData);
    } catch (ClassCastException | NoSuchElementException exception) {
      final TelemetryPayload telemetryErrors =
          TelemetryPayload.from(
              ImmutableTelemetryItem.builder()
                  .type("distinctDataError")
                  .data(
                      ImmutableMap.of(
                          "inputData",
                          roundTripData,
                          "exception",
                          ExceptionUtil.getSerializableExceptionData(exception)))
                  .build());
      return ImmutableS3SingleExtractedEvent.builder().telemetryErrors(telemetryErrors).build();
    }

    final ImmutableMap<String, String> baseDataFields =
        ImmutableMap.of(
            S3EventFieldName.ACTION_TYPE,
            distinctData.actionType().toWireString(),
            S3EventFieldName.TIMESTAMP,
            eventDateTimeISO8601,
            S3EventFieldName.S3_PROVIDER,
            s3Provider.toWireString());
    final ImmutableMap<String, Object> dataPayload =
        ImmutableMap.<String, Object>builder()
            .putAll(baseDataFields)
            .putAll(additionalPayloadFields)
            // There shouldn't be any overriding keys in the distinct fields,
            // but if there are, distinct field keys will take precedence
            .putAll(distinctData.distinctFields())
            .buildKeepingLast();

    return ImmutableS3SingleExtractedEvent.builder()
        .dataPayload(dataPayload)
        .telemetryWarnings(telemetryWarningsBuilder.build())
        .build();
  }
}
