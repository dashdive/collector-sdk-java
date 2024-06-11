package com.dashdive.internal.telemetry;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.dashdive.internal.DashdiveInstanceInfo;
import com.dashdive.internal.S3SingleExtractedEvent;
import org.immutables.value.Value;

@Value.Enclosing
public class TelemetryEvent {
  public static class Type {
    public static final String INVALID_API_KEY = "invalid_api_key";
    public static final String LIFECYCLE_STARTUP = "startup";
    public static final String LIFECYCLE_SHUTDOWN = "shutdown";
    public static final String EXTRACTION_ISSUES = "extraction_issues";
    public static final String METRICS_INCREMENTAL = "metrics_incremental";
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableTelemetryEvent.InvalidApiKey.class)
  public abstract static class InvalidApiKey {
    @Value.Derived
    public String eventType() {
      return Type.INVALID_API_KEY;
    }

    public abstract String instanceId();

    public abstract String apiKey();

    public abstract TelemetryPayload errors();
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableTelemetryEvent.LifecycleStartup.class)
  public abstract static class LifecycleStartup {
    @Value.Derived
    public String eventType() {
      return Type.LIFECYCLE_STARTUP;
    }

    public abstract String instanceId();

    public abstract DashdiveInstanceInfo instanceInfo();

    public abstract TelemetryPayload warnings();
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableTelemetryEvent.LifecycleShutdown.class)
  public abstract static class LifecycleShutdown {
    @Value.Derived
    public String eventType() {
      return Type.LIFECYCLE_SHUTDOWN;
    }

    public abstract String instanceId();

    public abstract ImmutableMap<String, Integer> metricsTotal();
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableTelemetryEvent.ExtractionIssues.class)
  public abstract static class ExtractionIssues {
    @Value.Derived
    public String eventType() {
      return Type.EXTRACTION_ISSUES;
    }

    public abstract String instanceId();

    public abstract ImmutableList<S3SingleExtractedEvent> eventsWithIssues();
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableTelemetryEvent.MetricsIncremental.class)
  public abstract static class MetricsIncremental {
    @Value.Derived
    public String eventType() {
      return Type.METRICS_INCREMENTAL;
    }

    public abstract String instanceId();

    public abstract ImmutableMap<String, Integer> metricsIncremental();
  }
}
