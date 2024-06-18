package com.dashdive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * An enumeration of all supported S3 providers that can be used with the Dashdive SDK. Each
 * provider corresponds to a specific S3-compatible storage service, and is used to categorize the
 * events that are sent to the Dashdive ingestion API.
 *
 * <p>Note that it is indeed possible, and even documented and encouraged, to use the AWS SDK with
 * other S3-compatible providers such as Backblaze B2 and Wasabi.
 */
public enum S3Provider {
  AWS("aws"),
  BACKBLAZE("backblaze"),
  WASABI("wasabi");

  private final String text;

  S3Provider(final String text) {
    this.text = text;
  }

  /**
   * Returns the provider that corresponds to the given text representation, which is expected to be
   * a "snake_case" version of the "CONSTANT_CASE" enum name.
   *
   * @param text a "snake_case" version of the "CONSTANT_CASE" enum name
   * @return the provider that corresponds to the given text representation, or {@link
   *     Optional#empty()} if the text representation is invalid
   */
  public static Optional<S3Provider> safeValueOf(String text) {
    if (!text.equals(text.toLowerCase())) {
      return Optional.empty();
    }
    String capitalized = text.toUpperCase();
    try {
      return Optional.of(S3Provider.valueOf(capitalized));
    } catch (IllegalArgumentException exception) {
      return Optional.empty();
    }
  }

  /**
   * Returns the text representation of the provider, a "snake_case" version of the "CONSTANT_CASE"
   * enum name.
   *
   * @return the text representation of the provider
   */
  @Override
  public String toString() {
    return text;
  }

  /**
   * Returns the wire representation of the provider, which is compatible with the Dashdive
   * ingestion API. In practice, this is the same as the enum's text representation via {@link
   * #toString()}.
   *
   * @return the wire representation of the provider
   */
  public String toWireString() {
    return text;
  }

  /**
   * An enumeration of S3 provider suffixes, which are used to infer the provider from an S3 bucket
   * endpoint URL.
   */
  public static enum Suffix {
    AWS("amazonaws.com"),
    BACKBLAZE("backblazeb2.com"),
    WASABI("wasabisys.com");

    private final String text;

    Suffix(final String text) {
      this.text = text;
    }

    @Override
    public String toString() {
      return text;
    }
  }

  /** A mapping of S3 provider suffixes to their corresponding providers. */
  public static final Map<S3Provider.Suffix, S3Provider> BY_SUFFIX =
      ImmutableMap.of(
          S3Provider.Suffix.AWS,
          S3Provider.AWS,
          S3Provider.Suffix.BACKBLAZE,
          S3Provider.BACKBLAZE,
          S3Provider.Suffix.WASABI,
          S3Provider.WASABI);

  /**
   * Infers the S3 provider from the given host, which is expected to be the endpoint URL of an
   * S3-compatible bucket. Note that the host must exactly end with the provider's suffix (e.g.
   * "amazonaws.com") in order to be inferred correctly.
   *
   * @param host the endpoint URL of an S3-compatible bucket
   * @return the inferred S3 provider, or {@link Optional#empty()} if the host does not match any
   *     known S3 provider
   */
  public static Optional<S3Provider> inferFrom(String host) {
    // [AWS] Endpoint URL takes the form "<potentially>.<many>.<tokens>.amazonaws.com"
    // https://docs.aws.amazon.com/general/latest/gr/s3.html
    //
    // [Backblaze] Endpoint URL takes the form "s3.<region>.backblazeb2.com",
    // where <region> is similar to `us-west-004`.
    // https://www.backblaze.com/apidocs/introduction-to-the-s3-compatible-api#prerequisites
    //
    // [Wasabi] Endpoint URL takes the form "s3.<region>.wasabisys.com" or "s3.wasabisys.com"
    // https://knowledgebase.wasabi.com/hc/en-us/articles/360015106031-What-are-the-service-URLs-for-Wasabi-s-different-storage-regions

    ImmutableSet<S3Provider> matchingProviders =
        S3Provider.BY_SUFFIX.entrySet().stream()
            .filter((entry) -> host.endsWith(entry.getKey().toString()))
            .map(Entry::getValue)
            .collect(ImmutableSet.toImmutableSet());

    return matchingProviders.size() == 1 ? matchingProviders.stream().findAny() : Optional.empty();
  }
}
