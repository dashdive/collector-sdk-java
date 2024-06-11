package com.dashdive;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableS3EventAttributeExtractorInput.class)
public abstract class S3EventAttributeExtractorInput {
  public abstract S3ActionType actionType();

  public abstract S3Provider s3Provider();

  public abstract Optional<String> bucketName();

  public abstract Optional<String> objectKey();
}
