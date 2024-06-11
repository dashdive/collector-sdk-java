package org.dashdive.internal;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableOSDistroInfo.class)
public abstract class OSDistroInfo {
  public abstract Optional<String> osName();

  public abstract Optional<String> osArch();

  public abstract Optional<String> osVersion();
}
