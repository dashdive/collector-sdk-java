package com.dashdive.internal;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class PriorityThreadFactory implements ThreadFactory {
  private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
  private final int priority;
  private final String namePrefix;

  public PriorityThreadFactory(int priority, String namePrefix) {
    this.priority = priority;
    this.namePrefix = namePrefix;
  }

  @Override
  public Thread newThread(Runnable r) {
    Thread thread = defaultFactory.newThread(r);
    thread.setPriority(priority);
    thread.setName(namePrefix + "-" + thread.getName());
    return thread;
  }
}
