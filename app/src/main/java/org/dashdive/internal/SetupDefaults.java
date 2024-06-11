package org.dashdive.internal;

import org.dashdive.internal.telemetry.TelemetryPayload;
import org.immutables.value.Value;

@Value.Immutable
public abstract class SetupDefaults {
  public abstract DashdiveInstanceInfo dashdiveInstanceInfo();

  public abstract int targetEventBatchSize();

  public abstract TelemetryPayload startupTelemetryWarnings();
}
