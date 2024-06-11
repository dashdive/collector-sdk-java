package com.dashdive.internal.batching;

import com.dashdive.ImmutableS3EventAttributeExtractorInput;
import com.dashdive.ImmutableS3EventAttributes;
import com.dashdive.S3ActionType;
import com.dashdive.S3EventAttributeExtractor;
import com.dashdive.S3EventAttributeExtractorInput;
import com.dashdive.S3EventAttributes;
import com.dashdive.S3Provider;
import com.dashdive.internal.ImmutableS3SingleExtractedEvent;
import com.dashdive.internal.S3EventFieldName;
import com.dashdive.internal.S3SingleExtractedEvent;
import com.dashdive.internal.telemetry.ImmutableTelemetryItem;
import com.dashdive.internal.telemetry.TelemetryItem;
import com.dashdive.internal.telemetry.TelemetryPayload;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Optional;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;

public class S3SingleEventProcessor {
  private static final Logger logger = LoggerFactory.getLogger(S3SingleEventProcessor.class);

  // - Region.of throws an error if input string is empty
  //   (See: `Validate.paramNotBlank(value, "region")`)
  // - Region.of with an unknown region does indeed work
  private static final ImmutableSet<String> validAwsRegionIds =
      Region.regions().stream().map((region) -> region.id()).collect(ImmutableSet.toImmutableSet());

  private S3SingleEventProcessor() {}

  private static S3SingleExtractedEvent processEvent_GetObject(
      S3SingleExtractedEvent event, Optional<String> machineRegion) {
    if (event.hasIrrecoverableErrors()) {
      return event;
    }

    final ImmutableMap<String, Object> dataPayload = event.dataPayload();

    final String egressFullHost =
        Optional.ofNullable(dataPayload.get(S3EventFieldName.Intermediate.EGRESS_FULL_HOST))
            .map(Object::toString)
            .orElse("");
    final HashMap<String, Object> updatedDataPayloadBuilder = new HashMap<>(dataPayload);
    updatedDataPayloadBuilder.remove(S3EventFieldName.Intermediate.EGRESS_FULL_HOST);

    final Optional<S3Provider> inferredS3Provider = S3Provider.inferFrom(egressFullHost);
    final boolean canBillableEgressBeFalse =
        inferredS3Provider.isPresent() && inferredS3Provider.get().equals(S3Provider.AWS);
    if (!canBillableEgressBeFalse) {
      // Telemetry for inability to infer S3 provider is taken care of earlier on
      return event;
    }

    final ImmutableList<String> s3HostParts = ImmutableList.copyOf(egressFullHost.split("\\."));
    final String inferredRegion =
        s3HostParts.size() >= 3 ? s3HostParts.get(s3HostParts.size() - 3) : "";
    if (inferredRegion.isEmpty() || !validAwsRegionIds.contains(inferredRegion)) {
      final TelemetryItem newWarning =
          ImmutableTelemetryItem.builder()
              .type("INFER_REGION_FAILURE")
              .data(
                  ImmutableMap.of(
                      "inferredRegion", inferredRegion, "egressFullHost", egressFullHost))
              .build();
      return ImmutableS3SingleExtractedEvent.builder()
          .from(event)
          .telemetryWarnings(
              TelemetryPayload.builder()
                  .add(newWarning)
                  .mergeFrom(event.telemetryWarnings())
                  .build())
          .build();
    }

    updatedDataPayloadBuilder.put(
        S3EventFieldName.IS_EGRESS_BILLABLE,
        !(machineRegion.isPresent() && machineRegion.get().equals(inferredRegion)));

    return ImmutableS3SingleExtractedEvent.builder()
        .from(event)
        .dataPayload(updatedDataPayloadBuilder)
        .build();
  }

  public static S3SingleExtractedEvent processAnyEvent(
      S3SingleExtractedEvent event,
      @Nullable S3EventAttributeExtractor s3EventAttributeExtractor,
      Optional<String> machineRegion) {
    final ImmutableMap<String, Object> dataPayload = event.dataPayload();
    final S3ActionType actionType =
        S3ActionType.safeValueOf(
                Optional.ofNullable(dataPayload.get(S3EventFieldName.ACTION_TYPE))
                    .map(Object::toString)
                    .orElse(""))
            .orElse(null);
    final S3Provider s3Provider =
        S3Provider.safeValueOf(
                Optional.ofNullable(dataPayload.get(S3EventFieldName.S3_PROVIDER))
                    .map(Object::toString)
                    .orElse(""))
            .orElse(null);

    if (actionType == null || s3Provider == null) {
      final TelemetryItem newError =
          ImmutableTelemetryItem.builder()
              .type("INFER_REQUIRED_FIELD_FAILURE")
              .data(
                  ImmutableMap.of(
                      "message", "Missing actionType or s3Provider", "dataPayload", dataPayload))
              .build();
      return ImmutableS3SingleExtractedEvent.builder()
          .from(event)
          .hasIrrecoverableErrors(true)
          .telemetryErrors(
              TelemetryPayload.builder().add(newError).mergeFrom(event.telemetryErrors()).build())
          .build();
    }

    final S3SingleExtractedEvent postProcessed =
        actionType == S3ActionType.GET_OBJECT
            ? processEvent_GetObject(event, machineRegion)
            : event;

    final ImmutableMap<String, Object> initEventData = postProcessed.dataPayload();
    final S3EventAttributeExtractorInput userAttrExtractionInput =
        ImmutableS3EventAttributeExtractorInput.builder()
            .actionType(actionType)
            .s3Provider(s3Provider)
            .bucketName(
                Optional.ofNullable((String) initEventData.get(S3EventFieldName.BUCKET_NAME)))
            .objectKey(Optional.ofNullable((String) initEventData.get(S3EventFieldName.OBJECT_KEY)))
            .build();
    final S3EventAttributes nullableUserExtractedAttributes =
        s3EventAttributeExtractor == null
            ? null
            : s3EventAttributeExtractor.extractAttributes(userAttrExtractionInput);
    final S3EventAttributes userExtractedAttributes =
        nullableUserExtractedAttributes == null
            ? ImmutableS3EventAttributes.of()
            : nullableUserExtractedAttributes;

    // Run various checks on the extracted attributes
    final TelemetryPayload.Builder userAttrWarnings = TelemetryPayload.builder();
    if (userExtractedAttributes.isEmpty()) {
      logger.warn(
          "Encountered empty S3EventAttributes for event [type={}]",
          postProcessed
              .dataPayload()
              .getOrDefault(S3EventFieldName.ACTION_TYPE, S3ActionType.UNKNOWN)
              .toString());
      userAttrWarnings.add(
          ImmutableTelemetryItem.builder()
              .type("EMPTY_USER_ATTRS")
              .data(
                  ImmutableMap.of(
                      "message",
                      "`userExtractedAttributes` was empty",
                      "inputArguments",
                      userAttrExtractionInput))
              .build());
    }
    if (actionType != S3ActionType.PUT_OBJECT
        && userExtractedAttributes.objectCategory().isPresent()
        && !userExtractedAttributes.objectCategory().get().isEmpty()) {
      userAttrWarnings.add(
          ImmutableTelemetryItem.builder()
              .type("UNEXPECTED_OBJECT_CATEGORY")
              .data(
                  ImmutableMap.of(
                      "message",
                      "`objectCategory` found on action type that is not PUT or POST",
                      "inputArguments",
                      userAttrExtractionInput,
                      "objectCategory",
                      userExtractedAttributes.objectCategory().get()))
              .build());
    }

    return ImmutableS3SingleExtractedEvent.builder()
        .from(postProcessed)
        .dataPayload(
            ImmutableMap.<String, Object>builder()
                .putAll(postProcessed.dataPayload())
                .putAll(userExtractedAttributes.asMap())
                // There shouldn't ever be overlap between the component maps,
                // but just in case, don't error out
                .buildKeepingLast())
        .telemetryWarnings(userAttrWarnings.mergeFrom(postProcessed.telemetryWarnings()).build())
        .build();
  }
}
