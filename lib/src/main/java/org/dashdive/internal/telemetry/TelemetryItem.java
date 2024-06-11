package org.dashdive.internal.telemetry;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableTelemetryItem.class)
public abstract class TelemetryItem {
  // General type, e.g. "NETWORK_ERROR"
  public abstract String type();

  // Indicates originating module or code path, e.g. "systemPropertiesFailure"
  public abstract Optional<String> domain();

  // NOTE: Assumes there are no fields "type" or "domain" in this data map;
  // otherwise the precedence behavior is undefined
  @JsonAnyGetter
  // Any data helpful in diagnosing the problem
  public abstract Map<String, Object> data();
}
