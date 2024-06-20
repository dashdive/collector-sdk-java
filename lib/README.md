# Internal Documentation

## Publishing to Maven Central

### One-Time Setup

As described in the [official](https://central.sonatype.org/publish/publish-guide/) and [unofficial](https://madhead.me/posts/no-bullshit-maven-publish/) guides:

- Create an account with Sonatype
- Verify your domain (e.g. `com.dashdive`) via DNS TXT records
- Generate a username/password publishing token pair
- Generate a public/private GPG key pair and publish the public part to key servers

### Recurring Setup (Checklist)

**NOTE**: If the integration tests are failing, either locally or in the cloud, with an error about incompatible Java version variants, this may be due to outdated builds in the local Maven repository. Delete the `dashdive` folder in the local Maven repository (usually at `~/.m2/repository/com/dashdive`) and try again. Also ensure that the correct SDK versions are specified in the integration test client Gradle build script.

1. Run local unit tests, local integration tests, and cloud integration tests with the current local build:

   ```bash
   # Local unit tests
   gradle clean :lib:test

   # Local integration tests
   ./integration_test/test-run.sh local

   # Cloud integration tests
   ./integration_test/test-setup.sh <ec2_ip>
   ssh -i integration_test/sdk-test.pem admin@<ec2_ip>
   ./integration_test/test-run.sh cloud  # from inside EC2 instance
   ```

1. Manually update all hard-coded current SDK versions in the repo via find and replace. This includes at least the following:

   - The public static member variable `VERSION` on the `Dashdive` class
   - The variable `dashdiveCurrentVersion` in the library build script, `build.gradle.kts`
   - The variables `dashdiveSdkVersion_centralManualTesting` in the integration client build script, `build.gradle.kts`
   - The analogous shell variables `VERSION_CENTRAL_MANUAL` and `VERSION_STANDARD` in the integration test script, `test-run.sh`

1. Publish the new build to the staging repository (reversible):

   ```bash
   ORG_GRADLE_PROJECT_signingInMemoryKey='<key>' \
   ORG_GRADLE_PROJECT_signingInMemoryKeyPassword='<password>' \
   ORG_GRADLE_PROJECT_mavenCentralUsername='<username>' \
   ORG_GRADLE_PROJECT_mavenCentralPassword='<password>' \
      gradle :lib:publishToMavenCentral --no-configuration-cache
   ```

1. Re-run the local and cloud integration tests with the deployed staging build:

   ```bash
   # Local integration tests
   BEARER_TOKEN="<sonatype_bearer>" ./integration_test/test-run.sh local -c

   # Cloud integration tests (from inside EC2 instance)
   BEARER_TOKEN="<sonatype_bearer>" ./integration_test/test-run.sh cloud -c
   ```

1. Release the build to Maven Central (**IRREVERSIBLE**).

1. Create a release tag for the newly published version on the corresponding commit on GitHub.

## Tests

A note on Gradle: if tests don't run even after running `gradle test`, it means the target is up-to-date. To force a test run, instead run `gradle clean test`.

### Unit Tests

We make an effort to separate the unit-testable portion of the program, i.e. the batching, telemetry, and metrics pipeline, from the parts which require integration, i.e. a connected `S3Client`, a connected `EC2MetadataClient`, and an HTTP connection to the Dashdive backend.

We unit test the pipeline by spoofing the input (`S3Client` method call) and by mocking the output (HTTP requests via `DashdiveConnection.send`). On the input side, we directly capture the `S3RoundTripInterceptor` which is ordinarily only invoked by the instrumented `S3Client`. Instead, we simulate real S3 calls by directly calling, with synthetic requests and responses that we've constructed:

```java
s3RoundTripInterceptorInstance.afterExecution(Context.AfterExecution context, ExecutionAttributes executionAttributes)
```

On the output side, in `TestUtils.java`, we implement a `MockHttpClient` that is injected in place of a real HTTP client. It records the HTTP requests sent by the pipeline, returning whatever responses are required by that particular test. The captured HTTP requests can be inspected and assertions can be made inside the tests to verify their content.

### Integration Tests

Since we've tested core functionality via unit tests, the only functionality remaining to be tested is:

1. That requests made by the user to the `S3Client` instance result in payloads sent to the interceptor (having fields `SdkRequest`/`SdkHttpRequest`/`SdkResponse`/`SdkHttpResponse`) that have the expected format
1. That `EC2MetadataClient` invocations behave expectedly, whether or not we're running on an EC2 instance (including proper setting of the `isBillableEgress` field)
1. All requests made to `S3Client` are sent roughly within "max age" time (test when less than a full batch size of requests is made)

For maximum consistency, we use the built and distributed version of the library. This means using a debug backdoor (`DashdiveConnection._setIngestBaseUri`) to override the ingestion endpoint from `ingest.dashdive.com` to a local URL where all the received payloads are logged or printed.

For now, these tests are done manually. Nonetheless, the Java programs and dummy ingest servers used to run the tests, both on and off an EC2 instance, are implemented in the integration tests folder.
