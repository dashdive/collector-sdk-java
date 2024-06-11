package com.dashdive.internal.extraction;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.immutables.value.Value;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

class SdkRequestSerializer extends StdSerializer<SdkRequest> {
  public SdkRequestSerializer() {
    super(SdkRequest.class);
  }

  @Override
  public void serialize(SdkRequest value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    @SuppressWarnings("unchecked")
    final Map<String, Object> attributesMap =
        value.sdkFields().stream()
            .map(
                item ->
                    (Pair<String, Object>)
                        Pair.of(item.memberName(), item.getValueOrDefault(value)))
            .filter(pair -> pair.getRight() != null)
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    gen.writeStartObject();
    for (Map.Entry<String, Object> entry : attributesMap.entrySet()) {
      gen.writeObjectField(entry.getKey(), entry.getValue().toString());
    }
    gen.writeEndObject();
  }
}

class SdkResponseSerializer extends StdSerializer<SdkResponse> {
  public SdkResponseSerializer() {
    super(SdkResponse.class);
  }

  @Override
  public void serialize(SdkResponse value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    @SuppressWarnings("unchecked")
    final Map<String, Object> attributesMap =
        value.sdkFields().stream()
            .map(
                item ->
                    (Pair<String, Object>)
                        Pair.of(item.memberName(), item.getValueOrDefault(value)))
            .filter(pair -> pair.getRight() != null)
            .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));

    gen.writeStartObject();
    for (Map.Entry<String, Object> entry : attributesMap.entrySet()) {
      gen.writeObjectField(entry.getKey(), entry.getValue().toString());
    }
    gen.writeEndObject();
  }
}

@Value.Immutable
@JsonSerialize(as = ImmutableS3RoundTripData.class)
public abstract class S3RoundTripData {
  @JsonSerialize(using = SdkRequestSerializer.class)
  public abstract SdkRequest pojoRequest();

  @JsonSerialize(using = SdkResponseSerializer.class)
  public abstract SdkResponse pojoResponse();

  public abstract SdkHttpRequest httpRequest();

  public abstract SdkHttpResponse httpResponse();
}
