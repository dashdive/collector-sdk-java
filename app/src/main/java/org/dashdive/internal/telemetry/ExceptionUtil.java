package org.dashdive.internal.telemetry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Optional;

public class ExceptionUtil {
  private static final ImmutableSet<String> STACK_TRACE_PREFIX_WHITELIST =
      ImmutableSet.of("software.amazon.awssdk", "org.dashdive");

  private static ImmutableList<ImmutableMap<String, String>> getSanitizedStack(
      Throwable exception) {
    return List.of(exception.getStackTrace()).stream()
        .filter(
            (elem) -> {
              final String className = elem.getClassName();
              return STACK_TRACE_PREFIX_WHITELIST.stream()
                  .anyMatch((prefix) -> className.startsWith(prefix));
            })
        .map(
            (elem) ->
                ImmutableMap.of(
                    "className", elem.getClassName(),
                    "fileName", Optional.ofNullable(elem.getFileName()).orElse(""),
                    "methodName", elem.getMethodName(),
                    "lineNumber", Integer.toString(elem.getLineNumber())))
        .collect(ImmutableList.toImmutableList());
  }

  public static ImmutableList<ImmutableMap<String, Object>> getSerializableExceptionData(
      Throwable finalException) {
    List<Throwable> exceptionChain =
        org.apache.commons.lang3.exception.ExceptionUtils.getThrowableList(finalException);
    return exceptionChain.stream()
        .map(
            exception ->
                ImmutableMap.of(
                    "exceptionMessage",
                    Optional.ofNullable(exception.getMessage()),
                    "exceptionType",
                    exception.getClass().getName(),
                    "exceptionStack",
                    getSanitizedStack(exception)))
        .collect(ImmutableList.toImmutableList());
  }
}
