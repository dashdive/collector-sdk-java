package com.dashdive;

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/**
 * An enumeration of all supported S3 action types that can be recorded by the Dashdive SDK. Each
 * action type corresponds to a specific S3 API operation, and is used to categorize the events that
 * are sent to the Dashdive ingestion API.
 *
 * <p>Unsupported action types can still be processed without errors and are represented by the
 * {@link #UNKNOWN} constant. These events are still sent to the Dashdive ingestion API.
 */
public enum S3ActionType {
  UNKNOWN("Unknown"),

  // "Object modification" event types
  GET_OBJECT("GetObject"),
  PUT_OBJECT("PutObject"),
  COPY_OBJECT("CopyObject"),
  DELETE_OBJECT("DeleteObject"),
  DELETE_OBJECTS("DeleteObjects"),

  // "Multipart upload" event types
  CREATE_MULTIPART_UPLOAD("CreateMultipartUpload"),
  COMPLETE_MULTIPART_UPLOAD("CompleteMultipartUpload"),
  ABORT_MULTIPART_UPLOAD("AbortMultipartUpload"),
  UPLOAD_PART("UploadPart"),
  UPLOAD_PART_COPY("UploadPartCopy"),
  LIST_PARTS("ListParts"),
  LIST_MULTIPART_UPLOADS("ListMultipartUploads"),

  // "Global" event types
  LIST_BUCKETS("ListBuckets"),

  // "Per-bucket" event types
  DELETE_BUCKET("DeleteBucket"),
  HEAD_BUCKET("HeadBucket"),
  CREATE_BUCKET("CreateBucket"),
  GET_BUCKET_ACL("GetBucketAcl"),
  GET_BUCKET_CORS("GetBucketCors"),
  GET_BUCKET_ENCRYPTION("GetBucketEncryption"),
  GET_BUCKET_LOCATION("GetBucketLocation"),
  GET_BUCKET_VERSIONING("GetBucketVersioning"),
  PUT_BUCKET_ACL("PutBucketAcl"),
  PUT_BUCKET_CORS("PutBucketCors"),
  PUT_BUCKET_ENCRYPTION("PutBucketEncryption"),
  DELETE_BUCKET_CORS("DeleteBucketCors"),
  DELETE_BUCKET_ENCRYPTION("DeleteBucketEncryption"),
  GET_OBJECT_LOCK_CONFIGURATION("GetObjectLockConfiguration"),
  PUT_OBJECT_LOCK_CONFIGURATION("PutObjectLockConfiguration"),
  LIST_OBJECTS("ListObjects"),
  LIST_OBJECTS_V2("ListObjectsV2"),
  LIST_OBJECT_VERSIONS("ListObjectVersions"),

  // "Simple per-object" event types
  HEAD_OBJECT("HeadObject"),
  GET_OBJECT_ACL("GetObjectAcl"),
  PUT_OBJECT_ACL("PutObjectAcl"),
  GET_OBJECT_RETENTION("GetObjectRetention"),
  PUT_OBJECT_RETENTION("PutObjectRetention"),
  GET_OBJECT_LEGAL_HOLD("GetObjectLegalHold"),
  PUT_OBJECT_LEGAL_HOLD("PutObjectLegalHold");

  private final String text;

  S3ActionType(final String text) {
    this.text = text;
  }

  /**
   * Returns the action type that corresponds to the given text representation, which is expected to
   * be a "TitleCase" version of the "CONSTANT_CASE" enum name.
   *
   * @param text a "TitleCase" version of the "CONSTANT_CASE" enum name
   * @return the action type that corresponds to the given text representation, or {@link
   *     Optional#empty()} if the text representation is invalid
   */
  public static Optional<S3ActionType> safeValueOf(String text) {
    if (!StringUtils.isAlphanumeric(text)) {
      return Optional.empty();
    }

    StringBuilder builder = new StringBuilder(text.length());
    for (int i = 0; i < text.length(); i++) {
      char curr = text.charAt(i);
      builder.append(curr);

      boolean isNextCaps = i + 1 < text.length() && Character.isUpperCase(text.charAt(i + 1));
      if (isNextCaps) {
        builder.append("_");
      }
    }
    String underscored = builder.toString().toUpperCase();

    try {
      return Optional.of(S3ActionType.valueOf(underscored));
    } catch (IllegalArgumentException exception) {
      return Optional.empty();
    }
  }

  /**
   * Returns the text representation of the action type, a "TitleCase" version of the
   * "CONSTANT_CASE" enum name.
   *
   * @return the text representation of the action type
   */
  @Override
  public String toString() {
    return text;
  }

  /**
   * Returns the wire representation of the action type, which is compatible with the Dashdive
   * ingestion API. In practice, this is the same as the enum's text representation via {@link
   * #toString()}.
   *
   * @return the wire representation of the action type
   */
  public String toWireString() {
    return text;
  }
}
