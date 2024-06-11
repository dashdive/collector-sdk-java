package com.dashdive;

import javax.annotation.Nonnull;

@FunctionalInterface
public interface S3EventAttributeExtractor {
  S3EventAttributes extractAttributes(@Nonnull S3EventAttributeExtractorInput input);
}

class NoOpS3EventAttributeExtractor implements S3EventAttributeExtractor {
  @Override
  public S3EventAttributes extractAttributes(@Nonnull S3EventAttributeExtractorInput input) {
    return ImmutableS3EventAttributes.of();
  }
}
