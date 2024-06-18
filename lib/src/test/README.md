A note on Gradle: if tests don't run even after running `gradle test`, it means the target is up-to-date. To force a test run, instead run `gradle clean test`.

## Unit Tests

We make an effort to separate the unit-testable portion of the program, i.e. the batching, telemetry, and metrics pipeline, from the parts which require integration, i.e. a connected `S3Client`, a connected `EC2MetadataClient`, and an HTTP connection to the Dashdive backend.

We unit test the pipeline by spoofing the input (`S3Client` method call) and by mocking the output (HTTP requests via `DashdiveConnection.send`). On the input side, we directly capture the `S3RoundTripInterceptor` which is ordinarily only invoked by the instrumented `S3Client`. Instead, we simulate real S3 calls by directly calling, with synthetic requests and responses that we've constructed:

```java
s3RoundTripInterceptorInstance.afterExecution(Context.AfterExecution context, ExecutionAttributes executionAttributes)
```

On the output side, in `TestUtils.java`, we implement a `MockHttpClient` that is injected in place of a real HTTP client. It records the HTTP requests sent by the pipeline, returning whatever responses are required by that particular test. The captured HTTP requests can be inspected and assertions can be made inside the tests to verify their content.

## Integration Tests

Since we've tested core functionality via unit tests, the only functionality remaining to be tested is:

1. That requests made by the user to the `S3Client` instance result in payloads sent to the interceptor (having fields `SdkRequest`/`SdkHttpRequest`/`SdkResponse`/`SdkHttpResponse`) that have the expected format
1. That `EC2MetadataClient` invocations behave expectedly, whether or not we're running on an EC2 instance (including proper setting of the `isBillableEgress` field)
1. All requests made to `S3Client` are sent roughly within "max age" time (test when less than a full batch size of requests is made)

For maximum consistency, we use the built and distributed version of the library. This means using a debug backdoor (`DashdiveConnection._setIngestBaseUri`) to override the ingestion endpoint from `ingest.dashdive.com` to a local URL where all the received payloads are logged or printed.

For now, these tests are done manually. Nonetheless, the Java programs and dummy ingest servers used to run the tests, both on and off an EC2 instance, are implemented in the integration tests folder.
