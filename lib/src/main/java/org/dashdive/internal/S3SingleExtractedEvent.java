package org.dashdive.internal;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import org.dashdive.internal.telemetry.TelemetryPayload;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableS3SingleExtractedEvent.class)
public abstract class S3SingleExtractedEvent {
  public abstract ImmutableMap<String, Object> dataPayload();

  // Redundant with `telemetryErrors`; should be true if and only if
  // `telemetryErrors` is non-empty. However, best practice is to indicate separtely.
  @Value.Default
  public boolean hasIrrecoverableErrors() {
    return false;
  }

  @Value.Default
  public TelemetryPayload telemetryWarnings() {
    return TelemetryPayload.of();
  }

  @Value.Default
  public TelemetryPayload telemetryErrors() {
    return TelemetryPayload.of();
  }
}
