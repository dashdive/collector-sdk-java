package org.dashdive;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map.Entry;
import java.util.Optional;

// Note that it is indeed possible, and documented / encouraged, to use
// the AWS SDK with other S3-compatible providers such as Backblaze B2 and Wasabi.
public enum S3Provider {
  AWS("aws"),
  BACKBLAZE("backblaze"),
  WASABI("wasabi");

  private final String text;

  S3Provider(final String text) {
    this.text = text;
  }

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

  @Override
  public String toString() {
    return text;
  }

  public String toWireString() {
    return text;
  }

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

  public static final ImmutableMap<S3Provider.Suffix, S3Provider> BY_SUFFIX =
      ImmutableMap.of(
          S3Provider.Suffix.AWS,
          S3Provider.AWS,
          S3Provider.Suffix.BACKBLAZE,
          S3Provider.BACKBLAZE,
          S3Provider.Suffix.WASABI,
          S3Provider.WASABI);

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
