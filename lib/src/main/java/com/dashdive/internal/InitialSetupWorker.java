package com.dashdive.internal;

import com.dashdive.Dashdive;
import com.dashdive.internal.batching.SingleEventBatcher;
import com.dashdive.internal.telemetry.ExceptionUtil;
import com.dashdive.internal.telemetry.ImmutableTelemetryEvent;
import com.dashdive.internal.telemetry.ImmutableTelemetryItem;
import com.dashdive.internal.telemetry.TelemetryEvent;
import com.dashdive.internal.telemetry.TelemetryPayload;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.retry.backoff.FixedDelayBackoffStrategy;
import software.amazon.awssdk.imds.Ec2MetadataClient;
import software.amazon.awssdk.imds.Ec2MetadataResponse;

public class InitialSetupWorker implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(InitialSetupWorker.class);
  private static final String IMDS_BASE_PATH = "/latest/meta-data/";

  private static final String partialUserAgent =
      DashdiveConnection.Headers.getUserAgent(
          Optional.empty(), Optional.of(Dashdive.VERSION), Optional.empty());
  private final HttpClient httpClient;
  private final String apiKey;

  private final Optional<SetupDefaults> skipSetupWithDefaults;
  private final boolean shouldSkipImdsQueries;

  private final AtomicBoolean isInitialized;
  private final AtomicReference<DashdiveInstanceInfo> instanceInfo;
  private final AtomicInteger targetEventBatchSize;

  private boolean hasRunPostSetupAction = false;
  private final Optional<Runnable> postSetupAction;

  public InitialSetupWorker(
      HttpClient httpClient,
      String apiKey,
      Optional<SetupDefaults> skipSetupWithDefaults,
      boolean shouldSkipImdsQueries,
      AtomicBoolean isInitialized,
      AtomicReference<DashdiveInstanceInfo> instanceInfo,
      AtomicInteger targetEventBatchSize,
      Optional<Runnable> postSetupAction) {
    this.httpClient = httpClient;
    this.apiKey = apiKey;

    this.skipSetupWithDefaults = skipSetupWithDefaults;
    this.shouldSkipImdsQueries = shouldSkipImdsQueries;

    this.isInitialized = isInitialized;
    this.instanceInfo = instanceInfo;
    this.targetEventBatchSize = targetEventBatchSize;

    this.postSetupAction = postSetupAction;
  }

  private void runPostSetupActionIdempotently() {
    if (!hasRunPostSetupAction) {
      hasRunPostSetupAction = true;
      postSetupAction.ifPresent(Runnable::run);
    }
  }

  private static TelemetryPayload checkIngestConnectionWithLogging(
      String apiKey, HttpClient httpClient) {
    final boolean isApiKeyFormatValid = DashdiveConnection.APIKey.isValid(apiKey);
    if (!isApiKeyFormatValid) {
      logger.error("Invalid API key format: '{}'", apiKey);
      return TelemetryPayload.from(
          ImmutableTelemetryItem.builder()
              .type("API_KEY_INVALID_FORMAT")
              .data(ImmutableMap.of("inputApiKey", apiKey, "dashdiveVersion", Dashdive.VERSION))
              .build());
    }

    final HttpRequest pingRequest =
        HttpRequest.newBuilder()
            .uri(DashdiveConnection.Routes.PING)
            .header(DashdiveConnection.Headers.USER_AGENT, partialUserAgent)
            .header(DashdiveConnection.Headers.API_KEY, apiKey)
            .GET()
            .build();
    try {
      final HttpResponse<String> pingResponse = DashdiveConnection.send(httpClient, pingRequest);
      final boolean didSucceed = pingResponse.statusCode() == HttpURLConnection.HTTP_OK;
      if (!didSucceed) {
        final ImmutableMap<String, Object> pingResponseParsed =
            ImmutableMap.of(
                "statusCode",
                pingResponse.statusCode(),
                "body",
                Optional.ofNullable(pingResponse.body()).orElse(""),
                "headers",
                pingResponse.headers().map());
        if (pingResponse.statusCode() == HttpURLConnection.HTTP_UNAUTHORIZED) {
          logger.error("API key unauthorized: '{}'", apiKey);
        } else {
          logger.error("Ingest ping failed with HTTP response: {}", pingResponseParsed);
        }
        return TelemetryPayload.from(
            ImmutableTelemetryItem.builder()
                .type("PING_FAILED")
                .data(
                    ImmutableMap.of(
                        "request",
                        ImmutableMap.of(
                            "route",
                            pingRequest.method() + " " + pingRequest.uri(),
                            "headers",
                            pingRequest.headers().map()),
                        "response",
                        pingResponseParsed,
                        "dashdiveVersion",
                        Dashdive.VERSION))
                .build());
      }
    } catch (IOException | InterruptedException exception) {
      logger.error("Ingest ping failed with exception", exception);
      return TelemetryPayload.from(
          ImmutableTelemetryItem.builder()
              .type("NETWORK_ERROR")
              .data(
                  ImmutableMap.of(
                      "exception",
                      ExceptionUtil.getSerializableExceptionData(exception),
                      "dashdiveVersion",
                      Dashdive.VERSION))
              .build());
    }
    return TelemetryPayload.of();
  }

  private void checkIngestConnectionWithReporting(String apiKey, HttpClient httpClient) {
    final TelemetryPayload checkIngestErrors = checkIngestConnectionWithLogging(apiKey, httpClient);
    if (!checkIngestErrors.isEmpty()) {
      try {
        final TelemetryEvent.InvalidApiKey invalidApiKeyPayload =
            ImmutableTelemetryEvent.InvalidApiKey.builder()
                .instanceId(instanceInfo.get().classInstanceId().orElse(""))
                .apiKey(apiKey)
                .errors(checkIngestErrors)
                .build();
        final ObjectMapper objectMapper = DashdiveConnection.DEFAULT_SERIALIZER;
        final String requestBodyJson = objectMapper.writeValueAsString(invalidApiKeyPayload);
        final HttpRequest invalidApiKeyRequest =
            HttpRequest.newBuilder()
                .uri(DashdiveConnection.Routes.TELEMETRY_API_KEY)
                .header(DashdiveConnection.Headers.USER_AGENT, partialUserAgent)
                // No API key header since, by virtue of this code path, there was an API key issue
                .POST(HttpRequest.BodyPublishers.ofString(requestBodyJson))
                .build();

        DashdiveConnection.send(httpClient, invalidApiKeyRequest);
      } catch (IOException | InterruptedException ignored) {
      }
    }
  }

  @Value.Immutable
  abstract static class BatchSizeResult {
    public abstract int targetEventBatchSize();

    @Value.Default
    public TelemetryPayload telemetryWarnings() {
      return TelemetryPayload.of();
    }
    ;
  }

  private static BatchSizeResult getTargetEventBatchSize(String apiKey, HttpClient httpClient) {
    final HttpRequest targetBatchSizeRequest =
        HttpRequest.newBuilder()
            .uri(DashdiveConnection.Routes.S3_RECOMMENDED_BATCH_SIZE)
            .header(DashdiveConnection.Headers.USER_AGENT, partialUserAgent)
            .header(DashdiveConnection.Headers.API_KEY, apiKey)
            .GET()
            .build();

    try {
      final HttpResponse<String> targetBatchSizeResponse =
          DashdiveConnection.send(httpClient, targetBatchSizeRequest);
      final boolean didSucceed = targetBatchSizeResponse.statusCode() == HttpURLConnection.HTTP_OK;
      final int targetEventBatchSize =
          didSucceed
              ? Integer.parseInt(targetBatchSizeResponse.body())
              : SingleEventBatcher.DEFAULT_TARGET_BATCH_SIZE;
      return ImmutableBatchSizeResult.builder().targetEventBatchSize(targetEventBatchSize).build();
    } catch (IOException | InterruptedException | NumberFormatException exception) {
      return ImmutableBatchSizeResult.builder()
          .targetEventBatchSize(SingleEventBatcher.DEFAULT_TARGET_BATCH_SIZE)
          .telemetryWarnings(
              TelemetryPayload.from(
                  ImmutableTelemetryItem.builder()
                      .type("NETWORK_ERROR")
                      .data(
                          ImmutableMap.of(
                              "exception", ExceptionUtil.getSerializableExceptionData(exception)))
                      .build()))
          .build();
    }
  }

  @Value.Immutable
  abstract static class GetAwsImdsDataResult {
    public abstract DashdiveInstanceInfo imdsData();

    public abstract TelemetryPayload telemetryWarnings();
  }

  // https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html#instance-metadata-ex-1
  // https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-categories.html
  private static class IMDSDataField {
    public static final String INSTANCE_ID = "instance-id";
    public static final String REGION = "placement/region";
    public static final String AVAILABILITY_ZONE = "placement/availability-zone";
    public static final String AVAILABILITY_ZONE_ID = "placement/availability-zone-id";
    public static final String PUBLIC_IPV4 = "public-ipv4";
    public static final String AMI_ID = "ami-id";
    public static final String KERNEL_ID = "kernel-id";
    public static final String INSTANCE_TYPE = "instance-type";
  }

  private static final ImmutableList<String> imdsDataFieldPaths =
      ImmutableList.of(
          IMDSDataField.INSTANCE_ID,
          IMDSDataField.REGION,
          IMDSDataField.AVAILABILITY_ZONE,
          IMDSDataField.AVAILABILITY_ZONE_ID,
          IMDSDataField.PUBLIC_IPV4,
          IMDSDataField.AMI_ID,
          IMDSDataField.KERNEL_ID,
          IMDSDataField.INSTANCE_TYPE);

  private static GetAwsImdsDataResult getAwsImdsData() {
    final int maxImdsRequestConcurrency = 8;
    // We don't use Ec2MetadataAsyncClient because it has noisy error
    // logging that we don't want to surface to our users
    final Ec2MetadataClient client =
        Ec2MetadataClient.builder()
            // Don't have to worry about retries blocking client
            //  startup since this is a background thread
            .retryPolicy(
                p ->
                    p.backoffStrategy(FixedDelayBackoffStrategy.create(Duration.ofMillis(200)))
                        .numRetries(2))
            .build();
    final ExecutorService executor = Executors.newFixedThreadPool(maxImdsRequestConcurrency);

    final ConcurrentMap<String, String> valuesByField = new ConcurrentHashMap<>();
    final ConcurrentMap<String, ImmutableList<ImmutableMap<String, Object>>>
        exceptionPayloadsByField = new ConcurrentHashMap<>();
    final ImmutableList<CompletableFuture<Void>> futures =
        imdsDataFieldPaths.stream()
            .map(
                imdsDataFieldPath ->
                    CompletableFuture.runAsync(
                        () -> {
                          try {
                            Ec2MetadataResponse response =
                                client.get(IMDS_BASE_PATH + imdsDataFieldPath);
                            valuesByField.put(imdsDataFieldPath, response.asString());
                          } catch (Exception exception) {
                            exceptionPayloadsByField.put(
                                imdsDataFieldPath,
                                ExceptionUtil.getSerializableExceptionData(exception));
                          }
                        },
                        executor))
            .collect(ImmutableList.toImmutableList());

    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    client.close();
    executor.shutdown();

    return ImmutableGetAwsImdsDataResult.builder()
        .imdsData(
            ImmutableDashdiveInstanceInfo.builder()
                .imdAmiId(Optional.ofNullable(valuesByField.get(IMDSDataField.AMI_ID)))
                .imdAvailabilityZone(
                    Optional.ofNullable(valuesByField.get(IMDSDataField.AVAILABILITY_ZONE)))
                .imdAvailabilityZoneId(
                    Optional.ofNullable(valuesByField.get(IMDSDataField.AVAILABILITY_ZONE_ID)))
                .imdEc2InstanceId(Optional.ofNullable(valuesByField.get(IMDSDataField.INSTANCE_ID)))
                .imdInstanceType(
                    Optional.ofNullable(valuesByField.get(IMDSDataField.INSTANCE_TYPE)))
                .imdKernelId(Optional.ofNullable(valuesByField.get(IMDSDataField.KERNEL_ID)))
                .imdPublicIpv4(Optional.ofNullable(valuesByField.get(IMDSDataField.PUBLIC_IPV4)))
                .imdRegion(Optional.ofNullable(valuesByField.get(IMDSDataField.REGION)))
                .build())
        .telemetryWarnings(
            TelemetryPayload.from(
                exceptionPayloadsByField.entrySet().stream()
                    .map(
                        fieldAndException ->
                            ImmutableTelemetryItem.builder()
                                .type("IMDS_ISSUE")
                                .data(
                                    ImmutableMap.of(
                                        "field",
                                        fieldAndException.getKey(),
                                        "exception",
                                        fieldAndException.getValue()))
                                .build())
                    .collect(ImmutableList.toImmutableList())))
        .build();
  }

  private TelemetryPayload doInitialSetup() {
    final TelemetryPayload.Builder startupTelemetryWarningsBuilder = TelemetryPayload.builder();
    final ImmutableDashdiveInstanceInfo.Builder instanceInfoBuilder =
        ImmutableDashdiveInstanceInfo.builder().from(instanceInfo.get());

    instanceInfoBuilder.logicalProcessorCount(Runtime.getRuntime().availableProcessors());

    CompletableFuture<Void> checkIngestFuture =
        CompletableFuture.runAsync(() -> checkIngestConnectionWithReporting(apiKey, httpClient));
    CompletableFuture<BatchSizeResult> batchSizeFuture =
        CompletableFuture.supplyAsync(() -> getTargetEventBatchSize(apiKey, httpClient));
    CompletableFuture<GetAwsImdsDataResult> imdsDataFuture =
        shouldSkipImdsQueries
            ? CompletableFuture.completedFuture(
                ImmutableGetAwsImdsDataResult.builder()
                    .imdsData(ImmutableDashdiveInstanceInfo.builder().build())
                    .telemetryWarnings(TelemetryPayload.of())
                    .build())
            : CompletableFuture.supplyAsync(() -> getAwsImdsData());
    CompletableFuture<Void> allFutures =
        CompletableFuture.allOf(checkIngestFuture, batchSizeFuture, imdsDataFuture);
    allFutures.join();

    final BatchSizeResult batchSizeResult = batchSizeFuture.join();
    final GetAwsImdsDataResult imdsDataResult = imdsDataFuture.join();
    instanceInfoBuilder.from(imdsDataResult.imdsData());

    startupTelemetryWarningsBuilder.mergeFrom(
        TelemetryPayload.builder()
            .mergeFrom(batchSizeResult.telemetryWarnings())
            .addDomainToAll("batchSizeFailure")
            .build());
    startupTelemetryWarningsBuilder.mergeFrom(
        TelemetryPayload.builder()
            .mergeFrom(imdsDataResult.telemetryWarnings())
            .addDomainToAll("imdsDataFailure")
            .build());

    try {
      final long pid = ProcessHandle.current().pid();
      instanceInfoBuilder.machinePid(String.valueOf(pid));
    } catch (SecurityException | UnsupportedOperationException exception) {
      startupTelemetryWarningsBuilder.add(
          ImmutableTelemetryItem.builder()
              .type("PID_FAILURE")
              .data(
                  ImmutableMap.of(
                      "exception", ExceptionUtil.getSerializableExceptionData(exception)))
              .build());
    }

    try {
      final OSDistroInfo osDistroInfo =
          ImmutableOSDistroInfo.builder()
              .osName(Optional.ofNullable(System.getProperty("os.name")))
              .osArch(Optional.ofNullable(System.getProperty("os.arch")))
              .osVersion(Optional.ofNullable(System.getProperty("os.version")))
              .build();
      instanceInfoBuilder.osDistroInfo(osDistroInfo);

      final Optional<String> javaVersion = Optional.ofNullable(System.getProperty("java.version"));
      instanceInfoBuilder.javaVersion(javaVersion);
    } catch (SecurityException exception) {
      startupTelemetryWarningsBuilder.add(
          ImmutableTelemetryItem.builder()
              .type("SYSTEM_PROPERTIES_FAILURE")
              .data(
                  ImmutableMap.of(
                      "exception", ExceptionUtil.getSerializableExceptionData(exception)))
              .build());
    }

    final DashdiveInstanceInfo instanceInfo = instanceInfoBuilder.build();
    final int targetEventBatchSize = batchSizeResult.targetEventBatchSize();
    this.instanceInfo.set(instanceInfo);
    this.targetEventBatchSize.set(targetEventBatchSize);
    this.isInitialized.set(true);

    return startupTelemetryWarningsBuilder.build();
  }

  private void runUncaught() {
    TelemetryPayload startupTelemetryWarnings;
    if (this.skipSetupWithDefaults.isEmpty()) {
      startupTelemetryWarnings = doInitialSetup();
    } else {
      final SetupDefaults setupDefaults = this.skipSetupWithDefaults.get();
      this.instanceInfo.set(setupDefaults.dashdiveInstanceInfo());
      this.targetEventBatchSize.set(setupDefaults.targetEventBatchSize());
      this.isInitialized.set(true);
      startupTelemetryWarnings = setupDefaults.startupTelemetryWarnings();
    }
    this.runPostSetupActionIdempotently();

    try {
      final ObjectMapper objectMapper = DashdiveConnection.DEFAULT_SERIALIZER;
      final TelemetryEvent.LifecycleStartup startupPayload =
          ImmutableTelemetryEvent.LifecycleStartup.builder()
              .instanceId(instanceInfo.get().classInstanceId().orElse(""))
              .instanceInfo(instanceInfo.get())
              .warnings(startupTelemetryWarnings)
              .build();
      final String requestBodyJson = objectMapper.writeValueAsString(startupPayload);
      final HttpRequest startupTelemetryRequest =
          HttpRequest.newBuilder()
              .uri(DashdiveConnection.Routes.TELEMETRY_LIFECYCLE)
              .header(DashdiveConnection.Headers.USER_AGENT, partialUserAgent)
              .header(DashdiveConnection.Headers.API_KEY, apiKey)
              .POST(HttpRequest.BodyPublishers.ofString(requestBodyJson))
              .build();
      DashdiveConnection.send(httpClient, startupTelemetryRequest);
    } catch (IOException | InterruptedException ignored) {
    }
  }

  @Override
  public void run() {
    try {
      runUncaught();
    } catch (Exception exception) {
      logger.error("Setup thread failed; will continue by using defaults.", exception);
      this.isInitialized.set(true);
      this.runPostSetupActionIdempotently();
    }
  }
}
