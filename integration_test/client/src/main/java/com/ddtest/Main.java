package com.ddtest;

import com.dashdive.Dashdive;
import com.dashdive.ImmutableS3EventAttributes;
import com.dashdive.S3EventAttributes;
import java.net.URI;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;

public class Main {
  // gradle :integration_test:client:run

  public static void main(String[] args) {
    final String API_KEY = "TESTKEYZ.yiXYc+TQZaJvcNq80KA7S6+eAfCPsW4kR59ooVAu3pj/Eqjo";
    final Dashdive dashdive =
        Dashdive.builder()
            .apiKey(API_KEY)
            .ingestionBaseUri(URI.create("http://127.0.0.1:3223"))
            .disableAllTelemetrySupplier(() -> true)
            .s3EventAttributeExtractor(
                input -> {
                  final S3EventAttributes result = ImmutableS3EventAttributes.builder().build();
                  System.out.println("input: " + input);
                  System.out.println("output: " + result);
                  return result;
                })
            .build();

    final ProfileCredentialsProvider specificCredentialsProvider =
        ProfileCredentialsProvider.create("StagingWrite");

    final S3Client s3Client =
        S3Client.builder()
            .region(Region.US_WEST_2)
            .credentialsProvider(specificCredentialsProvider)
            .overrideConfiguration(
                dashdive.addInterceptorTo(ClientOverrideConfiguration.builder()).build())
            .build();

    s3Client.listObjects(
        ListObjectsRequest.builder()
            .bucket("s3dashdivenotiftest")
            .prefix("test_fileprefix/")
            .build());

    s3Client.close();
    dashdive.close();
  }
}
