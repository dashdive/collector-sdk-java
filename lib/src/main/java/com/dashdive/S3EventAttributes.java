package com.dashdive;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable(singleton = true)
@JsonSerialize(as = ImmutableS3EventAttributes.class)
public abstract class S3EventAttributes {
  public abstract Optional<String> customerId();

  public abstract Optional<String> featureId();

  public abstract Optional<String> clientType();

  public abstract Optional<String> clientId();

  public abstract Optional<String> objectCategory();

  public Map<String, Object> asMap() {
    return Map.of(
            "customerId", customerId().orElse(""),
            "featureId", featureId().orElse(""),
            "clientType", clientType().orElse(""),
            "clientId", clientId().orElse(""),
            "objectCategory", objectCategory().orElse(""))
        .entrySet()
        .stream()
        .filter(entry -> entry.getValue() != null && !entry.getValue().isEmpty())
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }

  public boolean isEmpty() {
    return asMap().isEmpty();
  }
}
