package com.dashdive.internal;

public class S3EventFieldName {
  public static final String ACTION_TYPE = "action";
  public static final String TIMESTAMP = "timestamp";
  public static final String S3_PROVIDER = "provider";

  public static final String BUCKET_NAME = "bucket";

  public static final String OBJECT_KEY = "objectKey";
  public static final String OBJECT_VERSION_ID = "versionId";
  public static final String OBJECT_SIZE_BYTES = "bytes";

  public static final String IS_EGRESS_BILLABLE = "isBillableEgress";

  public static final String COPY_SOURCE_OBJECT = "copySource";

  public static final String DELETE_SOFT_ID = "resultingDeleteMarkerVersionId";
  public static final String DELETE_HARD_ID = "hardDeletedVersionId";
  public static final String DELETED_OBJECTS = "deletedObjects";

  public static final String MULTIPART_UPLOAD_ID = "uploadId";
  public static final String MULTIPART_PART_NUMBER = "partNumber";

  public static class Intermediate {
    // For inferring the region of the bucket for the `isBillableEgress` field
    public static final String EGRESS_FULL_HOST = "_egressFullHost";
  }

  private S3EventFieldName() {}
}
