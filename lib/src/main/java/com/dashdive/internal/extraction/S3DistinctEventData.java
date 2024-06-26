package com.dashdive.internal.extraction;

import com.dashdive.S3ActionType;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableS3DistinctEventData.class)
abstract class S3DistinctEventData {
  public abstract S3ActionType actionType();

  public abstract ImmutableMap<String, Object> distinctFields();
}
