package com.dashdive;

import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import org.immutables.builder.Builder;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.services.s3.S3ClientBuilder;


/**
 * An instance of the Dashdive collector, which is used to instrument one or multiple AWS {@link
 * software.amazon.awssdk.services.s3.S3Client} instances. Each instance of this class maintains a
 * separate connection to the Dashdive ingestion API, a separate event processing pipeline with
 * multiple thread pools, and a separate metrics collector. In practice, most users will only need
 * to instantiate this class once per application lifecycle.
 */
@Value.Style(newBuilder = "builder")
public class Dashdive implements AutoCloseable {
  /** The version of the Dashdive SDK. */
  public static final String VERSION = "1.0.2";
  public static final URI DEFAULT_INGEST_BASE_URI = URI.create("https://ingest.dashdive.com");

  private static final Logger logger = LoggerFactory.getLogger(Dashdive.class);
  private final Optional<DashdiveImpl> delegate;

  /**
   * Construct a new {@link Dashdive} instance with the provided API key, S3 event attribute
   * extractor factory, and shutdown grace period.
   *
   * @param apiKey the API key to use for sending data
   * @param ingestionBaseUri the base URI to use for sending data
   * @param s3EventAttributeExtractor a factory which returns a function which extracts attributes
   *     from S3 events
   * @param shutdownGracePeriod the duration to wait for the Dashdive instance to flush any
   *     remaining events and send
   */
   @Builder.Constructor
   public Dashdive(
      // All fields are optional to avoid NullPointerExceptions at runtime;
      // much better to have sends simply fail, for example, when apiKey is not specified
      Optional<URI> ingestionBaseUri,
      Optional<String> apiKey,
      Optional<S3EventAttributeExtractor> s3EventAttributeExtractor,
      Optional<Duration> shutdownGracePeriod) {
        if (!ingestionBaseUri.isPresent() || !apiKey.isPresent() || !s3EventAttributeExtractor.isPresent()) {
          logger.warn("Dashdive instance missing one or more required fields (will no-op):\n" +
            "{}: {}\n{}: {}\n{}: {}\n",
            "API_KEY", apiKey.isPresent() ? "present" : "absent",
            "INGEST_URI", ingestionBaseUri.isPresent() ? "present" : "absent",
            "ATTRIBUTE_EXTRACTOR", s3EventAttributeExtractor.isPresent() ? "present" : "absent");
          this.delegate = Optional.empty();
        } else {
          this.delegate = Optional.of(new DashdiveImpl(
            ingestionBaseUri, apiKey, s3EventAttributeExtractor, shutdownGracePeriod));
        }
      }

  /**
   * Construct a new {@link DashdiveBuilder} instance, which is used to configure and create a new
   * {@link Dashdive} instance.
   *
   * @return a new {@link DashdiveBuilder} instance
   */
  public static DashdiveBuilder builder() {
    return DashdiveBuilder.builder();
  }

  /**
   * Close the Dashdive instance, flushing any remaining events that have been collected. This
   * method is idempotent.
   *
   * <p>This immediately prevents any further events from being collected. Events previously
   * collected, but not yet sent, will block the close operation either for the duration of the
   * grace period, or indefinitely if no grace period was set. For reference, see {@link
   * DashdiveBuilder#shutdownGracePeriod(Duration)}.
   *
   * <p><b>NOTE</b>: This class is {@link AutoCloseable}, so in many instances, such as in Spring
   * projects, this method need not be called explicitly.
   */
  public void close() {
    this.delegate.ifPresent(d -> d.close());
  }

  /**
   * Add the Dashdive S3 event interceptor to the provided configuration builder. This method is
   * idempotent, so calling it multiple times on the same configuration builder will add the
   * interceptor exactly once.
   *
   * @param overrideConfigBuilder the configuration builder to add the interceptor to
   * @return the provided configuration builder with the Dashdive S3 event interceptor added
   */
  public ClientOverrideConfiguration.Builder addInterceptorTo(
      final ClientOverrideConfiguration.Builder overrideConfigBuilder) {
    return this.delegate.map(d -> d.addInterceptorTo(overrideConfigBuilder)).orElse(overrideConfigBuilder);
  }

  /**
   * Add the Dashdive S3 event interceptor to the provided S3 client builder. This method creates a
   * new {@link ClientOverrideConfiguration} with only the Dashdive S3 event interceptor, and sets
   * it on the provided client builder. It is a convenience method; in other words, the following
   * are equivalent:
   *
   * <pre>{@code
   * // Convenient version
   * S3Client s3Client = dashdive.addConfigWithInterceptorTo(S3Client.builder()).build()
   *
   * // Verbose version
   * S3Client s3Client = S3Client.builder()
   *      .overrideConfiguration(
   *            dashdive.addInterceptorTo(ClientOverrideConfiguration.builder()).build())
   *      .build()
   * }</pre>
   *
   * <b>NOTE</b>:
   *
   * <ul>
   *   <li>This method creates an entirely new {@link ClientOverrideConfiguration}, so any existing
   *       override configuration on the client builder will be erased, if previously set.
   *   <li>Once this method is called on the client builder, subsequent calls to {@link
   *       S3ClientBuilder#overrideConfiguration(ClientOverrideConfiguration)} will erase the
   *       interceptor.
   * </ul>
   *
   * @param clientBuilder the S3 client builder to add the interceptor to
   * @return the provided S3 client builder with the Dashdive S3 event interceptor added
   */
  public S3ClientBuilder addConfigWithInterceptorTo(final S3ClientBuilder clientBuilder) {
    return this.delegate.map(d -> d.addConfigWithInterceptorTo(clientBuilder)).orElse(clientBuilder);
  }
}
