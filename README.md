# Dashdive SDKs

## Overview

These SDKs are wrappers over SDKs for common cloud providers and cloud services. They are intended to be drop-in replacements, requiring no code changes other than configuration (i.e. injection of Dashdive API key, logger, and functions to extract attributes like `customer_id` from application context). They function identically to the wrapped SDKs, but they also send to Dashdive all events invoked by the underlying SDK.

Currently, only the AWS SDK for S3 in Java is supported. Work is underway to add support for additional services and languages.

## Installation

### From Maven Central

### Build From Source

## API

### Logging

This SDK uses [`SLF4J`](https://slf4j.org/) for logging and will respect any application-wide SLF4J configuration. Currently, the only non-error/non-warning logs are (1) a single `INFO` level log on startup, and (2) a single `INFO` level log on shutdown containing metrics collected over the lifetime of the instrumented `S3Client`.

### Version Compatbility

| Release Tag | Version Start | Version End | Notable Changes                                                                                                                                                                                                                                                                                                                                                             |
| ----------- | ------------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2.19.29     | 2.19.29       | 2.25.69     | - [[2.20.32](https://github.com/aws/aws-sdk-java-v2/blob/master/changelogs/2.20.x-CHANGELOG.md#22032-2023-03-24)] Getter `serviceClientConfiguration()` added to client, enabling listing of attached interceptors<br> - [[2.19.29](https://github.com/aws/aws-sdk-java-v2/issues/61#issuecomment-1416230962)] Non-internal `Ec2MetadataClient` replaces `Ec2MetadataUtils` |
| 2.17.136    | 2.17.136      | 2.19.28     | - [[2.17.136](https://github.com/aws/aws-sdk-java-v2/blob/master/changelogs/2.17.x-CHANGELOG.md#aws-sdk-for-java-v2-104)] Getter `overrideConfiguration` added to client builder, allowing in-place modification                                                                                                                                                            |
| 2.17.3      | 2.17.3        | 2.17.135    | None (earliest version)                                                                                                                                                                                                                                                                                                                                                     |
