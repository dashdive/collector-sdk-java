# Dashdive SDKs

## Overview

These SDKs are wrappers over SDKs for common cloud providers and cloud services. They are intended to be drop-in replacements, requiring no code changes other than configuration (i.e. injection of Dashdive API key, logger, and functions to extract attributes like `customer_id` from application context). They function identically to the wrapped SDKs, but they also send to Dashdive all events invoked by the underlying SDK.

Currently, only the AWS SDK for S3 in Java is supported. Work is underway to add support for additional services and languages.

## Installation

### From Maven Central

### Build From Source

```bash
gradle :lib:publishToMavenLocal
```

## API

### Logging

This SDK uses [`SLF4J`](https://slf4j.org/) for logging and will respect any application-wide SLF4J configuration. Currently, the only non-error/non-warning logs are (1) a single `INFO` level log on startup, and (2) a single `INFO` level log on shutdown containing metrics collected over the lifetime of the instrumented `S3Client`.

### Version Compatbility

| Release Tag | Version Start | Version End | Notable Changes                                                                                                                                                                                                                                                                                                                                                             |
| ----------- | ------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2.19.29     | 2.19.29       | 2.25.69     | - [[2.20.32](https://github.com/aws/aws-sdk-java-v2/blob/master/changelogs/2.20.x-CHANGELOG.md#22032-2023-03-24)] Getter `serviceClientConfiguration()` added to client, enabling listing of attached interceptors<br> - [[2.19.29](https://github.com/aws/aws-sdk-java-v2/issues/61#issuecomment-1416230962)] Non-internal `Ec2MetadataClient` replaces `Ec2MetadataUtils` |
| 2.17.136    | 2.17.136      | 2.19.28     | - [[2.17.136](https://github.com/aws/aws-sdk-java-v2/blob/master/changelogs/2.17.x-CHANGELOG.md#aws-sdk-for-java-v2-104)] Getter `overrideConfiguration` added to client builder, allowing in-place modification                                                                                                                                                            |
| 2.17.3      | 2.17.3        | 2.17.135    | None (earliest version)                                                                                                                                                                                                                                                                                                                                                     |

## Smoke Testing

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
