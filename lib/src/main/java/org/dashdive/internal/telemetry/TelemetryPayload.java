package org.dashdive.internal.telemetry;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TelemetryPayload {
  private final ImmutableList<TelemetryItem> items;

  @JsonValue
  public ImmutableList<TelemetryItem> getItems() {
    return items;
  }

  private TelemetryPayload(ImmutableList<TelemetryItem> items) {
    this.items = items;
  }

  public static class Builder {
    private List<TelemetryItem> items = new ArrayList<>();

    private Builder() {}

    public Builder add(TelemetryItem item) {
      items.add(item);
      return this;
    }

    public Builder addAll(List<TelemetryItem> items) {
      this.items.addAll(items);
      return this;
    }

    public Builder addDomainToAll(String domain) {
      this.items =
          this.items.stream()
              .map(item -> ImmutableTelemetryItem.builder().from(item).domain(domain).build())
              .collect(Collectors.toList());
      return this;
    }

    public Builder mergeFrom(TelemetryPayload otherPayload) {
      this.items.addAll(otherPayload.items);
      return this;
    }

    public TelemetryPayload build() {
      return new TelemetryPayload(ImmutableList.copyOf(items));
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static TelemetryPayload of() {
    return builder().build();
  }

  public boolean isEmpty() {
    return items.isEmpty();
  }

  public static TelemetryPayload from(TelemetryItem item) {
    return builder().add(item).build();
  }

  public static TelemetryPayload from(List<TelemetryItem> items) {
    return builder().addAll(items).build();
  }
}
