# Dashdive SDKs

## Overview

These SDKs are wrappers over SDKs for common cloud providers and cloud services. They are intended to be drop-in replacements, requiring no code changes other than configuration (i.e. injection of Dashdive API key, logger, and functions to extract attributes like `customer_id` from application context). They function identically to the wrapped SDKs, but they also send to Dashdive all events invoked by the underlying SDK.

Currently, only the AWS SDK for S3 in Java is supported. Work is underway to add support for additional services and languages.

## Building and Releasing

Ensure that `dashdiveSdkVersion` in `lib/build.gradle` and `final String VERSION` in `class Dashdive` match and are set to latest. Then run:

```bash
gradle :lib:publishAllPublicationsToMavenRepository
```

## Installation

### From Maven Central

### Build From Source

Local publishing works out of the box with the following command:

```bash
gradle :lib:publishToMavenLocal
```

Or, you can directly generate the compiled JAR, sources JAR, and Javadoc using each of the following:

```bash
gradle :lib:jar :lib:sourcesJar :lib:javadoc
```

Depending on your build system, you may find it convenient to create a different task in `lib/build.gradle.kts`.

## API

The full Javadoc reference is available [at this link](https://www.javadoc.io/doc/com.dashdive/collector-sdk/latest/com/dashdive/package-summary.html).

### Example Usage

```java
/* In `CustomExtractor.java` */
public class CustomExtractor implements S3EventAttributeExtractor {
  // This is an example of an extractor function, but
  // actual logic could be very different from this
  @Override
  public ImmutableS3EventAttributes extractAttributes(S3EventAttributeExtractorInput input) {
    if (input.s3Provider() != S3Provider.AWS) {
      // In this example, perhaps we only do inference for AWS S3 buckets
      return ImmutableS3EventAttributes.builder().build();
    }

    String customerId = input.bucketName().get();

    List<String> keyComponents = List.of(input.objectKey().orElse("").split("/"));
    Optional<String> featureId =
        keyComponents.size() > 0 ? Optional.of(keyComponents.get(0)) : Optional.empty();
    Optional<String> objectCategory =
        keyComponents.size() > 1 && input.actionType() == S3ActionType.PUT_OBJECT
            ? Optional.of(keyComponents.get(1))
            : Optional.empty();

    // Note that not all fields need be set
    return ImmutableS3EventAttributes.builder()
        .customerId(customerId)
        .objectCategory(objectCategory)
        .featureId(featureId)
        .build();
  }
}


/* Somewhere else when setting up an S3Client instance, perhaps for dependency injection */
Dashdive dashdive =
    Dashdive.builder()
        .apiKey("<api_key>") // Load your API key from a secure location
        .s3EventAttributeExtractor(new CustomExtractor()) // Or, more concisely, with a lambda function
        .shutdownGracePeriod(Optional.empty()) // Equivalent to leaving blank; ensures all events are sent
        .build();

S3Client s3Client =
    S3Client.builder()
        .region(Region.US_EAST_2) // Any region will do
        // ... various other builder fields ...
        .overrideConfiguration(
            dashdive.addInterceptorTo(ClientOverrideConfiguration.builder()).build())
        .build();
```

**NOTE**: The `close()` method **must** be called on the `Dashdive` instance before the program can shut down successfully. Otherwise, indefinitely long shutdown times may result. This is because the event processing pipeline managed by the `Dashdive` object contains executor thread pools with non-zero minimum thread counts, and these must be explicitly freed.

### Logging

This SDK uses [`SLF4J`](https://slf4j.org/) for logging and will respect any application-wide SLF4J configuration. Currently, the only non-error/non-warning logs are (1) a single `INFO` level log on startup, and (2) a single `INFO` level log on shutdown containing metrics collected over the lifetime of the instrumented `S3Client`.

### Version Notes

| Version Start | Version End | Notable Changes                                                                                                                                                                                                                                                                                                                                                             |
| ------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2.19.29       | 2.25.69     | - [[2.20.32](https://github.com/aws/aws-sdk-java-v2/blob/master/changelogs/2.20.x-CHANGELOG.md#22032-2023-03-24)] Getter `serviceClientConfiguration()` added to client, enabling listing of attached interceptors<br> - [[2.19.29](https://github.com/aws/aws-sdk-java-v2/issues/61#issuecomment-1416230962)] Non-internal `Ec2MetadataClient` replaces `Ec2MetadataUtils` |
| 2.17.136      | 2.19.28     | - [[2.17.136](https://github.com/aws/aws-sdk-java-v2/blob/master/changelogs/2.17.x-CHANGELOG.md#aws-sdk-for-java-v2-104)] Getter `overrideConfiguration` added to client builder, allowing in-place modification                                                                                                                                                            |
| 2.17.3        | 2.17.135    | None (earliest version)                                                                                                                                                                                                                                                                                                                                                     |

## Testing

### Unit Tests

```bash
gradle clean :lib:test  # from root directory
```

### Integration Tests

These tests ensure the wrapper works end to end in exactly the same way it would in a production deployment. A local Node.js server - which simply writes all received requests to a file - is started to emulate the Dashdive ingestion endpoint, and the target URL is changed using an internal debug method. The test then confirms that the request paylods written to the file have the expected format.

#### Running Locally

```bash
./integration_test/test-run.sh local
```

#### Running in EC2

- Create an EC2 instance in the `us-west-2` region with an SSH key called `sdk-test.pem`
- Ensure the `sdk-test.pem` key is in the `integration_test` directory

**IMPORTANT NOTE:** Running `./test-setup.sh` will copy your AWS credentials via `aws configure` and put them in the `.bashrc` on the EC2 instance.

```bash
./integration_test/test-setup.sh <ec2_ip>
ssh -i integration_test/sdk-test.pem admin@<ec2_ip>
./integration_test/test-run.sh cloud  # from inside EC2 instance
```

## Architecture Overview

The Dashdive SDK has 3 primary goals:

- **Ease of use.** Just a few lines are necessary to instrument existing clients, without sweeping codebase changes.
- **High performance.** Even in high-throughput scenarios, collecting and sending events to Dashdive constitute a small percentage of overall resource usage.
- **Data integrity.** Error recovery and retry mechanisms are implemented throughout the event processing pipeline. When dropped events are truly inevitable, the discrepancy should be reflected in both logs and metrics.

The event processing pipeline looks like this:

1. Dashdive's collector is injected into the client via [`ExecutionInterceptor`s](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/core/interceptor/ExecutionInterceptor.html). Each time an S3 request/response pair is generated, it is sent to Dashdive event processing pipeline via these interceptors.
1. Each thread does separate preliminary processing of a collected event (see `S3RoundTripExtractor`) and adds the pre-processed event to a per-thread queue (see `SingleEventBatcher`).
   - The `S3Client` provided by the AWS SDK is thread safe, and Dashdive preserves this thread safety.
   - In highly multithreaded scenarios, this approach minimizes lock contention on the downstream "global" (per `Dashdive` object instance) queue which handles sending event batches to the Dashdive backend. This has a beneficial effect on performance (see `ParallelPerfTest.java`).
1. Periodically, once a certain event count (100) or maximum age (2 seconds) has been reached in a per-thread queue, that batch is written from the per-thread queue to the global shared queue.
1. Batches of events are dequeued from the shared queue by a background worker thread pool which handles both post-processing, such as inferring attributes like `customerId`, (see `S3SingleEventProcessor`) and sending events to the Dashdive backend. This module ensures data integrity by retrying batches that failed to send, recording metrics on dropped and sent events, and sending additional telemetry, including information about errors encountered (see `BatchEventProcessor`).
