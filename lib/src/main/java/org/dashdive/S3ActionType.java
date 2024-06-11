package com.dashdive;

import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

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

  @Override
  public String toString() {
    return text;
  }

  public String toWireString() {
    return text;
  }
}
