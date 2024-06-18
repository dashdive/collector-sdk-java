package com.dashdive;

import javax.annotation.Nonnull;

/**
 * An interface for extracting attributes from an S3 event. An implementation of this interface
 * should be passed to the {@link Dashdive} instance during construction. The {@link Dashdive}
 * instance uses the implementation to automatically infer attributes such as {@link
 * S3EventAttributes#customerId()} and {@link S3EventAttributes#featureId()} from each S3 event.
 *
 * <p><b>Implementations of this interface must be stateless and thread-safe</b>, since the {@link
 * extractAttributes} method may be called concurrently by multiple threads. Each S3 event is
 * intended to be processed in isolation, so shared state across multiple events is strongly
 * discouraged. However, it may be necessary to invoke a stateful operation, such as an external
 * service or database call, to retrieve information related to a specific event. In such cases, if
 * the instrumented {@link software.amazon.awssdk.services.s3.S3Client} may be called from multiple
 * threads, it is strongly recommended to do one of the following:
 *
 * <ul>
 *   <li>Use a thread-safe client or synchronize access to the client.
 *   <li>Use separate clients or state within each thread, using a concurrent map or {@link
 *       java.lang.ThreadLocal}.
 *   <li>Invoke the calls in such a way to remain entirely stateless.
 * </ul>
 */
@FunctionalInterface
public interface S3EventAttributeExtractor {
  /**
   * Extracts attributes from an S3 event.
   *
   * @param input the input to the extractor
   * @return the extracted attributes
   */
  S3EventAttributes extractAttributes(@Nonnull S3EventAttributeExtractorInput input);
}

class NoOpS3EventAttributeExtractor implements S3EventAttributeExtractor {
  @Override
  public S3EventAttributes extractAttributes(@Nonnull S3EventAttributeExtractorInput input) {
    return ImmutableS3EventAttributes.of();
  }
}
