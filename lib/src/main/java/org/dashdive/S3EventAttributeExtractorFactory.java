package org.dashdive;

// Each extractor will be created an called for exactly one
// batch of events. Note that it is expected for multiple
// extractors to be created and called in parallel, since
// event batch processing is multithreaded.
@FunctionalInterface
public interface S3EventAttributeExtractorFactory {
  S3EventAttributeExtractor createExtractor();

  static S3EventAttributeExtractorFactory from(S3EventAttributeExtractor extractor) {
    return () -> extractor;
  }
}
