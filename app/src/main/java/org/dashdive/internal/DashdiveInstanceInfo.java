package org.dashdive.internal;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

// `imd` prefix indicates data from the AWS Instance Metadata Service
@Value.Immutable
@JsonSerialize(as = ImmutableDashdiveInstanceInfo.class)
public abstract class DashdiveInstanceInfo {
  // Identifying info for this particular invocation
  public abstract Optional<String> classInstanceId();

  public abstract Optional<String> machinePid();

  public abstract Optional<String> imdEc2InstanceId();

  public abstract Optional<String> imdPublicIpv4();

  // Info that helps determine billing

  public abstract Optional<String> imdRegion();

  public abstract Optional<String> imdAvailabilityZone();

  public abstract Optional<String> imdAvailabilityZoneId();

  // General non-crucial info about this machine

  public abstract Optional<Integer> logicalProcessorCount();

  public abstract Optional<String> imdAmiId();

  public abstract Optional<String> imdKernelId();

  public abstract Optional<String> imdInstanceType();

  public abstract Optional<OSDistroInfo> osDistroInfo();

  public abstract Optional<String> javaVersion();
}
