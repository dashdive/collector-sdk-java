package com.dashdive.internal;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class PriorityThreadFactory implements ThreadFactory {
    private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();
    private final int priority;

    public PriorityThreadFactory(int priority) {
        this.priority = priority;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = defaultFactory.newThread(r);
        thread.setPriority(priority);
        return thread;
    }
}