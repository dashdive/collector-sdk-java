package com.dashdive.internal.telemetry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ExceptionUtil {
  private static final ImmutableSet<String> STACK_TRACE_PREFIX_WHITELIST =
      ImmutableSet.of("software.amazon.awssdk", "com.dashdive");

  public static final String STACK__CLASS_NAME = "className";
  public static final String STACK__FILE_NAME = "fileName";
  public static final String STACK__METHOD_NAME = "methodName";
  public static final String STACK__LINE_NUMBER = "lineNumber";

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
                    STACK__CLASS_NAME, elem.getClassName(),
                    STACK__FILE_NAME, Optional.ofNullable(elem.getFileName()).orElse(""),
                    STACK__METHOD_NAME, elem.getMethodName(),
                    STACK__LINE_NUMBER, Integer.toString(elem.getLineNumber())))
        .collect(ImmutableList.toImmutableList());
  }

  public static String getChainWithoutStacks(Throwable finalException) {
    final List<Throwable> exceptionChain =
        org.apache.commons.lang3.exception.ExceptionUtils.getThrowableList(finalException);
    return String.join(
        "\n",
        exceptionChain.stream()
            .map(
                exception ->
                    exception.getClass().getName()
                        + ": "
                        + Optional.ofNullable(exception.getMessage()).orElse("_"))
            .collect(Collectors.toCollection(ArrayList::new)));
  }

  public static ImmutableList<ImmutableMap<String, Object>> getSerializableExceptionData(
      Throwable finalException) {
    final List<Throwable> exceptionChain =
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
