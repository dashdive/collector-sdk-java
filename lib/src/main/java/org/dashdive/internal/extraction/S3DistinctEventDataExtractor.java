package com.dashdive.internal.extraction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.NoSuchElementException;
import java.util.Optional;
import com.dashdive.S3ActionType;
import com.dashdive.internal.S3EventFieldName;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketCorsRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketCorsResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.GetBucketCorsRequest;
import software.amazon.awssdk.services.s3.model.GetBucketCorsResponse;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.GetBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.GetBucketLocationRequest;
import software.amazon.awssdk.services.s3.model.GetBucketLocationResponse;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.GetBucketVersioningResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAclRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectLegalHoldRequest;
import software.amazon.awssdk.services.s3.model.GetObjectLegalHoldResponse;
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationRequest;
import software.amazon.awssdk.services.s3.model.GetObjectLockConfigurationResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRetentionRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRetentionResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ListPartsRequest;
import software.amazon.awssdk.services.s3.model.ListPartsResponse;
import software.amazon.awssdk.services.s3.model.PutBucketAclRequest;
import software.amazon.awssdk.services.s3.model.PutBucketAclResponse;
import software.amazon.awssdk.services.s3.model.PutBucketCorsRequest;
import software.amazon.awssdk.services.s3.model.PutBucketCorsResponse;
import software.amazon.awssdk.services.s3.model.PutBucketEncryptionRequest;
import software.amazon.awssdk.services.s3.model.PutBucketEncryptionResponse;
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest;
import software.amazon.awssdk.services.s3.model.PutObjectAclResponse;
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldRequest;
import software.amazon.awssdk.services.s3.model.PutObjectLegalHoldResponse;
import software.amazon.awssdk.services.s3.model.PutObjectLockConfigurationRequest;
import software.amazon.awssdk.services.s3.model.PutObjectLockConfigurationResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRetentionRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRetentionResponse;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class S3DistinctEventDataExtractor {
  @VisibleForTesting
  public static final String _serializeRoundTripClassNames(
      String requestClassName, String responseClassName) {
    return serializeRoundTripClassNames(requestClassName, responseClassName);
  }

  private static final String serializeRoundTripClassNames(
      String requestClassName, String responseClassName) {
    return requestClassName + "," + responseClassName;
  }

  private static final ImmutableMap<String, S3ActionType> POJO_EVENT_NAME_TO_TYPE;

  static {
    ImmutableMap.Builder<String, S3ActionType> pojoMapBuilder = ImmutableMap.builder();

    // "Object modification" event types
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            GetObjectRequest.class.getName(), GetObjectResponse.class.getName()),
        S3ActionType.GET_OBJECT);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            PutObjectRequest.class.getName(), PutObjectResponse.class.getName()),
        S3ActionType.PUT_OBJECT);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            CopyObjectRequest.class.getName(), CopyObjectResponse.class.getName()),
        S3ActionType.COPY_OBJECT);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            DeleteObjectRequest.class.getName(), DeleteObjectResponse.class.getName()),
        S3ActionType.DELETE_OBJECT);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            DeleteObjectsRequest.class.getName(), DeleteObjectsResponse.class.getName()),
        S3ActionType.DELETE_OBJECTS);

    // "Multipart upload" event types
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            CreateMultipartUploadRequest.class.getName(),
            CreateMultipartUploadResponse.class.getName()),
        S3ActionType.CREATE_MULTIPART_UPLOAD);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            CompleteMultipartUploadRequest.class.getName(),
            CompleteMultipartUploadResponse.class.getName()),
        S3ActionType.COMPLETE_MULTIPART_UPLOAD);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            AbortMultipartUploadRequest.class.getName(),
            AbortMultipartUploadResponse.class.getName()),
        S3ActionType.ABORT_MULTIPART_UPLOAD);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            UploadPartRequest.class.getName(), UploadPartResponse.class.getName()),
        S3ActionType.UPLOAD_PART);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            UploadPartCopyRequest.class.getName(), UploadPartCopyResponse.class.getName()),
        S3ActionType.UPLOAD_PART_COPY);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            ListPartsRequest.class.getName(), ListPartsResponse.class.getName()),
        S3ActionType.LIST_PARTS);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            ListMultipartUploadsRequest.class.getName(),
            ListMultipartUploadsResponse.class.getName()),
        S3ActionType.LIST_MULTIPART_UPLOADS);

    // "Global" event types
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            ListBucketsRequest.class.getName(), ListBucketsResponse.class.getName()),
        S3ActionType.LIST_BUCKETS);

    // "Per-bucket" event types
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            DeleteBucketRequest.class.getName(), DeleteBucketResponse.class.getName()),
        S3ActionType.DELETE_BUCKET);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            HeadBucketRequest.class.getName(), HeadBucketResponse.class.getName()),
        S3ActionType.HEAD_BUCKET);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            CreateBucketRequest.class.getName(), CreateBucketResponse.class.getName()),
        S3ActionType.CREATE_BUCKET);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            GetBucketAclRequest.class.getName(), GetBucketAclResponse.class.getName()),
        S3ActionType.GET_BUCKET_ACL);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            GetBucketCorsRequest.class.getName(), GetBucketCorsResponse.class.getName()),
        S3ActionType.GET_BUCKET_CORS);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            GetBucketEncryptionRequest.class.getName(),
            GetBucketEncryptionResponse.class.getName()),
        S3ActionType.GET_BUCKET_ENCRYPTION);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            GetBucketLocationRequest.class.getName(), GetBucketLocationResponse.class.getName()),
        S3ActionType.GET_BUCKET_LOCATION);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            GetBucketVersioningRequest.class.getName(),
            GetBucketVersioningResponse.class.getName()),
        S3ActionType.GET_BUCKET_VERSIONING);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            PutBucketAclRequest.class.getName(), PutBucketAclResponse.class.getName()),
        S3ActionType.PUT_BUCKET_ACL);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            PutBucketCorsRequest.class.getName(), PutBucketCorsResponse.class.getName()),
        S3ActionType.PUT_BUCKET_CORS);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            PutBucketEncryptionRequest.class.getName(),
            PutBucketEncryptionResponse.class.getName()),
        S3ActionType.PUT_BUCKET_ENCRYPTION);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            DeleteBucketCorsRequest.class.getName(), DeleteBucketCorsResponse.class.getName()),
        S3ActionType.DELETE_BUCKET_CORS);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            DeleteBucketEncryptionRequest.class.getName(),
            DeleteBucketEncryptionResponse.class.getName()),
        S3ActionType.DELETE_BUCKET_ENCRYPTION);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            GetObjectLockConfigurationRequest.class.getName(),
            GetObjectLockConfigurationResponse.class.getName()),
        S3ActionType.GET_OBJECT_LOCK_CONFIGURATION);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            PutObjectLockConfigurationRequest.class.getName(),
            PutObjectLockConfigurationResponse.class.getName()),
        S3ActionType.PUT_OBJECT_LOCK_CONFIGURATION);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            ListObjectsV2Request.class.getName(), ListObjectsV2Response.class.getName()),
        S3ActionType.LIST_OBJECTS_V2);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            ListObjectVersionsRequest.class.getName(), ListObjectVersionsResponse.class.getName()),
        S3ActionType.LIST_OBJECT_VERSIONS);

    // "Simple per-object" event types
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            HeadObjectRequest.class.getName(), HeadObjectResponse.class.getName()),
        S3ActionType.HEAD_OBJECT);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            GetObjectAclRequest.class.getName(), GetObjectAclResponse.class.getName()),
        S3ActionType.GET_OBJECT_ACL);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            PutObjectAclRequest.class.getName(), PutObjectAclResponse.class.getName()),
        S3ActionType.PUT_OBJECT_ACL);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            GetObjectRetentionRequest.class.getName(), GetObjectRetentionResponse.class.getName()),
        S3ActionType.GET_OBJECT_RETENTION);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            PutObjectRetentionRequest.class.getName(), PutObjectRetentionResponse.class.getName()),
        S3ActionType.PUT_OBJECT_RETENTION);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            GetObjectLegalHoldRequest.class.getName(), GetObjectLegalHoldResponse.class.getName()),
        S3ActionType.GET_OBJECT_LEGAL_HOLD);
    pojoMapBuilder.put(
        serializeRoundTripClassNames(
            PutObjectLegalHoldRequest.class.getName(), PutObjectLegalHoldResponse.class.getName()),
        S3ActionType.PUT_OBJECT_LEGAL_HOLD);

    POJO_EVENT_NAME_TO_TYPE = pojoMapBuilder.buildKeepingLast();
  }

  public static S3DistinctEventData emptyEventData() {
    return ImmutableS3DistinctEventData.builder().actionType(S3ActionType.UNKNOWN).build();
  }

  public static S3DistinctEventData getFrom(S3RoundTripData roundTripData)
      throws ClassCastException, NoSuchElementException {
    // There are multiple names for a class, but `getName` is unique:
    // https://stackoverflow.com/a/15203417/14816795
    final String requestClassName = roundTripData.pojoRequest().getClass().getName();
    final String responseClassName = roundTripData.pojoResponse().getClass().getName();
    final String serializedReqResClassNames =
        serializeRoundTripClassNames(requestClassName, responseClassName);
    final S3ActionType actionType =
        POJO_EVENT_NAME_TO_TYPE.getOrDefault(serializedReqResClassNames, S3ActionType.UNKNOWN);

    // Using ImmutableMap, which throws on null value, is a feature, not a bug,
    // because we expect all unchecked values to be present (non-null). If they're
    // null, we should indeed halt and throw an exception.
    ImmutableMap<String, Object> distinctFields = ImmutableMap.of();
    // Switching on strings is very performant in Java, roughly O(1)
    // See: https://stackoverflow.com/a/22110821/14816795
    switch (actionType) {
        // "Object modification" event types
      case S3ActionType.GET_OBJECT:
        distinctFields =
            extractDistinctFieldsForEventType_GetObject(
                (GetObjectRequest) roundTripData.pojoRequest(),
                (GetObjectResponse) roundTripData.pojoResponse(),
                roundTripData.httpRequest());
        break;
      case S3ActionType.PUT_OBJECT:
        distinctFields =
            extractDistinctFieldsForEventType_PutObject(
                (PutObjectRequest) roundTripData.pojoRequest(),
                (PutObjectResponse) roundTripData.pojoResponse());
        break;
      case S3ActionType.COPY_OBJECT:
        distinctFields =
            extractDistinctFieldsForEventType_CopyObject(
                (CopyObjectRequest) roundTripData.pojoRequest(),
                (CopyObjectResponse) roundTripData.pojoResponse());
        break;
      case S3ActionType.DELETE_OBJECT:
        distinctFields =
            extractDistinctFieldsForEventType_DeleteObject(
                (DeleteObjectRequest) roundTripData.pojoRequest(),
                (DeleteObjectResponse) roundTripData.pojoResponse());
        break;
      case S3ActionType.DELETE_OBJECTS:
        distinctFields =
            extractDistinctFieldsForEventType_DeleteObjects(
                (DeleteObjectsRequest) roundTripData.pojoRequest(),
                (DeleteObjectsResponse) roundTripData.pojoResponse());
        break;

        // "Global" event types
      case S3ActionType.LIST_BUCKETS:
        distinctFields =
            extractDistinctFieldsForEventTypes_Global(
                roundTripData.pojoRequest(), roundTripData.pojoResponse());
        break;

        // "Multipart upload" event types
      case S3ActionType.CREATE_MULTIPART_UPLOAD:
        distinctFields =
            extractDistinctFieldsForEventType_CreateMultipartUpload(
                (CreateMultipartUploadRequest) roundTripData.pojoRequest(),
                (CreateMultipartUploadResponse) roundTripData.pojoResponse());
        break;
      case S3ActionType.COMPLETE_MULTIPART_UPLOAD:
        distinctFields =
            extractDistinctFieldsForEventType_CompleteMultipartUpload(
                (CompleteMultipartUploadRequest) roundTripData.pojoRequest(),
                (CompleteMultipartUploadResponse) roundTripData.pojoResponse());
        break;
      case S3ActionType.ABORT_MULTIPART_UPLOAD:
        distinctFields =
            extractDistinctFieldsForEventType_AbortMultipartUpload(
                (AbortMultipartUploadRequest) roundTripData.pojoRequest(),
                (AbortMultipartUploadResponse) roundTripData.pojoResponse());
        break;
      case S3ActionType.UPLOAD_PART:
        distinctFields =
            extractDistinctFieldsForEventType_UploadPart(
                (UploadPartRequest) roundTripData.pojoRequest(),
                (UploadPartResponse) roundTripData.pojoResponse());
        break;
      case S3ActionType.UPLOAD_PART_COPY:
        distinctFields =
            extractDistinctFieldsForEventType_UploadPartCopy(
                (UploadPartCopyRequest) roundTripData.pojoRequest(),
                (UploadPartCopyResponse) roundTripData.pojoResponse());
        break;
      case S3ActionType.LIST_PARTS:
        distinctFields =
            extractDistinctFieldsForEventType_ListParts(
                (ListPartsRequest) roundTripData.pojoRequest(),
                (ListPartsResponse) roundTripData.pojoResponse());
        break;
      case S3ActionType.LIST_MULTIPART_UPLOADS:
        distinctFields =
            extractDistinctFieldsForEventType_ListMultipartUploads(
                (ListMultipartUploadsRequest) roundTripData.pojoRequest(),
                (ListMultipartUploadsResponse) roundTripData.pojoResponse());
        break;

        // "Per-bucket" event types (all contain `bucket()` accessor)
      case S3ActionType.DELETE_BUCKET:
      case S3ActionType.HEAD_BUCKET:
      case S3ActionType.CREATE_BUCKET:
      case S3ActionType.GET_BUCKET_ACL:
      case S3ActionType.GET_BUCKET_CORS:
      case S3ActionType.GET_BUCKET_ENCRYPTION:
      case S3ActionType.GET_BUCKET_LOCATION:
      case S3ActionType.GET_BUCKET_VERSIONING:
      case S3ActionType.PUT_BUCKET_ACL:
      case S3ActionType.PUT_BUCKET_CORS:
      case S3ActionType.PUT_BUCKET_ENCRYPTION:
      case S3ActionType.DELETE_BUCKET_CORS:
      case S3ActionType.DELETE_BUCKET_ENCRYPTION:
      case S3ActionType.GET_OBJECT_LOCK_CONFIGURATION:
      case S3ActionType.PUT_OBJECT_LOCK_CONFIGURATION:
      case S3ActionType.LIST_OBJECTS_V2:
      case S3ActionType.LIST_OBJECT_VERSIONS:
        distinctFields =
            extractDistinctFieldsForMultipleEventTypes_PerBucket(
                roundTripData.pojoRequest(), roundTripData.pojoResponse());
        break;

        // "Simple per-object" event types (all contain `key()` accessor)
      case S3ActionType.HEAD_OBJECT:
      case S3ActionType.GET_OBJECT_ACL:
      case S3ActionType.PUT_OBJECT_ACL:
      case S3ActionType.GET_OBJECT_RETENTION:
      case S3ActionType.PUT_OBJECT_RETENTION:
      case S3ActionType.GET_OBJECT_LEGAL_HOLD:
      case S3ActionType.PUT_OBJECT_LEGAL_HOLD:
        distinctFields =
            extractDistinctFieldsForMultipleEventTypes_SimplePerObject(
                roundTripData.pojoRequest(), roundTripData.pojoResponse());
        break;

      case S3ActionType.UNKNOWN:
        distinctFields = ImmutableMap.of("classNames", serializedReqResClassNames);
        break;

      default:
        break;
    }

    return ImmutableS3DistinctEventData.builder()
        .actionType(actionType)
        .distinctFields(distinctFields)
        .build();
  }

  // "Object modification" event types

  private static ImmutableMap<String, Object> extractDistinctFieldsForEventType_GetObject(
      GetObjectRequest pojoRequest, GetObjectResponse pojoResponse, SdkHttpRequest httpRequest) {
    final String safeObjectVersionId = Optional.ofNullable(pojoRequest.versionId()).orElse("");
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME, pojoRequest.bucket(),
        S3EventFieldName.OBJECT_KEY, pojoRequest.key(),
        S3EventFieldName.OBJECT_VERSION_ID, safeObjectVersionId,
        S3EventFieldName.OBJECT_SIZE_BYTES, pojoResponse.contentLength(),
        S3EventFieldName.Intermediate.EGRESS_FULL_HOST, httpRequest.host());
  }

  private static ImmutableMap<String, Object> extractDistinctFieldsForEventType_PutObject(
      PutObjectRequest request, PutObjectResponse response) {
    final String safeObjectVersionId = Optional.ofNullable(response.versionId()).orElse("");
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME, request.bucket(),
        S3EventFieldName.OBJECT_KEY, request.key(),
        S3EventFieldName.OBJECT_VERSION_ID, safeObjectVersionId,
        S3EventFieldName.OBJECT_SIZE_BYTES, request.contentLength());
  }

  private static ImmutableMap<String, Object> extractDistinctFieldsForEventType_CopyObject(
      CopyObjectRequest request, CopyObjectResponse response) {
    final String safeDestVersionId = Optional.ofNullable(response.versionId()).orElse("");
    final String safeSourceVersionId = Optional.ofNullable(request.sourceVersionId()).orElse("");
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME,
        request.destinationBucket(),
        S3EventFieldName.OBJECT_KEY,
        request.destinationKey(),
        S3EventFieldName.OBJECT_VERSION_ID,
        safeDestVersionId,
        S3EventFieldName.COPY_SOURCE_OBJECT,
        ImmutableMap.of(
            S3EventFieldName.BUCKET_NAME, request.sourceBucket(),
            S3EventFieldName.OBJECT_KEY, request.sourceKey(),
            S3EventFieldName.OBJECT_VERSION_ID, safeSourceVersionId));
  }

  private static ImmutableMap<String, Object> extractDistinctFieldsForEventType_DeleteObject(
      DeleteObjectRequest request, DeleteObjectResponse response) {
    final String safeSoftVersionId = Optional.ofNullable(response.versionId()).orElse("");
    final String safeHardVersionId = Optional.ofNullable(request.versionId()).orElse("");
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME,
        request.bucket(),
        S3EventFieldName.OBJECT_KEY,
        request.key(),
        S3EventFieldName.DELETE_SOFT_ID,
        safeSoftVersionId,
        S3EventFieldName.DELETE_HARD_ID,
        safeHardVersionId);
  }

  private static ImmutableMap<String, Object> extractDistinctFieldsForEventType_DeleteObjects(
      DeleteObjectsRequest request, DeleteObjectsResponse response) {
    /*
     * - If "VersionId" is present, the object (whether data obj or marker obj) was hard deleted (ignore "DeleteMarkerVersionId" if present)
     * - If "VersionId" is not present, a new delete marker was created with ID "DeleteMarkerVersionId"
     * - "DeleteMarker" flag can be ignored entirely
     */

    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME, request.bucket(),
        S3EventFieldName.DELETED_OBJECTS,
            response.deleted().stream()
                .map(
                    (deletedObj) -> {
                      final String safeVersionId =
                          Optional.ofNullable(deletedObj.versionId()).orElse("");
                      final String safeDeleteMarkerVersionId =
                          Optional.ofNullable(deletedObj.deleteMarkerVersionId()).orElse("");
                      return ImmutableMap.of(
                          S3EventFieldName.OBJECT_KEY,
                          deletedObj.key(),
                          safeVersionId.isEmpty()
                              ? S3EventFieldName.DELETE_SOFT_ID
                              : S3EventFieldName.DELETE_HARD_ID,
                          safeVersionId.isEmpty() ? safeDeleteMarkerVersionId : safeVersionId);
                    })
                .collect(ImmutableList.toImmutableList()));
  }

  // "Multipart upload" event types

  private static ImmutableMap<String, Object>
      extractDistinctFieldsForEventType_CreateMultipartUpload(
          CreateMultipartUploadRequest request, CreateMultipartUploadResponse response) {
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME, request.bucket(),
        S3EventFieldName.MULTIPART_UPLOAD_ID, response.uploadId(),
        S3EventFieldName.OBJECT_KEY, request.key());
  }

  private static ImmutableMap<String, Object>
      extractDistinctFieldsForEventType_CompleteMultipartUpload(
          CompleteMultipartUploadRequest request, CompleteMultipartUploadResponse response) {
    final String safeObjectVersionId = Optional.ofNullable(response.versionId()).orElse("");
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME, request.bucket(),
        S3EventFieldName.MULTIPART_UPLOAD_ID, request.uploadId(),
        S3EventFieldName.OBJECT_KEY, request.key(),
        S3EventFieldName.OBJECT_VERSION_ID, safeObjectVersionId);
  }

  private static ImmutableMap<String, Object>
      extractDistinctFieldsForEventType_AbortMultipartUpload(
          AbortMultipartUploadRequest request, AbortMultipartUploadResponse response) {
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME, request.bucket(),
        S3EventFieldName.MULTIPART_UPLOAD_ID, request.uploadId());
  }

  private static ImmutableMap<String, Object> extractDistinctFieldsForEventType_UploadPart(
      UploadPartRequest request, UploadPartResponse response) {
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME, request.bucket(),
        S3EventFieldName.MULTIPART_UPLOAD_ID, request.uploadId(),
        S3EventFieldName.MULTIPART_PART_NUMBER, request.partNumber(),
        S3EventFieldName.OBJECT_SIZE_BYTES, request.contentLength());
  }

  private static ImmutableMap<String, Object> extractDistinctFieldsForEventType_UploadPartCopy(
      UploadPartCopyRequest request, UploadPartCopyResponse response) {
    final String safeSourceVersionId = Optional.ofNullable(request.sourceVersionId()).orElse("");
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME, request.destinationBucket(),
        S3EventFieldName.MULTIPART_UPLOAD_ID, request.uploadId(),
        S3EventFieldName.MULTIPART_PART_NUMBER, request.partNumber(),
        S3EventFieldName.COPY_SOURCE_OBJECT,
            ImmutableMap.of(
                S3EventFieldName.BUCKET_NAME, request.sourceBucket(),
                S3EventFieldName.OBJECT_KEY, request.sourceKey(),
                S3EventFieldName.OBJECT_VERSION_ID, safeSourceVersionId));
  }

  private static ImmutableMap<String, Object> extractDistinctFieldsForEventType_ListParts(
      ListPartsRequest request, ListPartsResponse response) {
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME, request.bucket(),
        S3EventFieldName.MULTIPART_UPLOAD_ID, request.uploadId());
  }

  private static ImmutableMap<String, Object>
      extractDistinctFieldsForEventType_ListMultipartUploads(
          ListMultipartUploadsRequest request, ListMultipartUploadsResponse response) {
    return ImmutableMap.of(S3EventFieldName.BUCKET_NAME, request.bucket());
  }

  // "Global" event types

  private static ImmutableMap<String, Object> extractDistinctFieldsForEventTypes_Global(
      SdkRequest request, SdkResponse response) {
    return ImmutableMap.of();
  }

  // "Per-bucket" event types

  private static ImmutableMap<String, Object> extractDistinctFieldsForMultipleEventTypes_PerBucket(
      SdkRequest request, SdkResponse response) throws NoSuchElementException {
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME,
        request.getValueForField("Bucket", String.class).orElseThrow());
  }

  // "Simple per-object" event types

  private static ImmutableMap<String, Object>
      extractDistinctFieldsForMultipleEventTypes_SimplePerObject(
          SdkRequest request, SdkResponse response) throws NoSuchElementException {
    return ImmutableMap.of(
        S3EventFieldName.BUCKET_NAME, request.getValueForField("Key", String.class).orElseThrow());
  }
}
