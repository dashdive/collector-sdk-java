package com.dashdive;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * An interface representing the fields present on an S3 event that can be used as input when
 * inferring attributes related to that event. The provided implementation of this interface is the
 * {@link ImmutableS3EventAttributeExtractorInput} class, which is a {@link Value.Immutable} class
 * generated by the Immutables library, with an associated {@link
 * ImmutableS3EventAttributeExtractorInput.Builder} builder class.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableS3EventAttributeExtractorInput.class)
public abstract class S3EventAttributeExtractorInput {
  /**
   * The type of S3 API call performed.
   *
   * @return the type of S3 API call performed
   */
  public abstract S3ActionType actionType();

  /**
   * The S3 provider that corresponds to the storage service used.
   *
   * @return the S3 provider that corresponds to the storage service used
   */
  public abstract S3Provider s3Provider();

  /**
   * The name of the bucket associated with the S3 event, if applicable.
   *
   * @return the name of the bucket associated with the S3 event
   */
  public abstract Optional<String> bucketName();

  /**
   * The key of the object associated with the S3 event, if applicable.
   *
   * @return the key of the object associated with the S3 event
   */
  public abstract Optional<String> objectKey();
}
