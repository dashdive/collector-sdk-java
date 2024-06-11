package org.dashdive.internal.batching;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.tuple.Pair;
import org.dashdive.internal.S3EventFieldName;
import org.dashdive.internal.S3SingleExtractedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Useful comparable implementation in Datadog's DogStatsD Java client:
 * https://github.com/DataDog/java-dogstatsd-client/blob/master/src/main/java/com/timgroup/statsd/StatsDAggregator.java
 *
 * They don't use per-thread batching; instead, they have a fixed variable determining the number of batches.
 * They hash each event and map it to a batch, then synchronize on that batch. This is less robust because it
 * scales less dynamically with the number of threads. It works comparably for just a few threads, assuming the
 * batch count is also small, but with many threads, there will be contention on the batch locks.
 */

/*
 * Naive approach: delete and recreate after each use
 *
 * Counterexample:
 * - Thread 1 creates lock L1 and acquires it
 * - Thread 2 waits for L1
 * - Thread 1 releases L1 and deletes it
 * - Thread 2 now acquires L1 and begins execution of protected code
 * - Thread 3 sees no lock, creating and acquiring L2 and begins execution of protected code
 * Now Thread 2 and Thread 3 are simultaneously executing, which is prohibited.
 *
 *
 * Robust approach: atomic reference counting
 *
 * - Thread 1 creates lock L1 with refcount 1 and acquires it
 * - Thread 2 waits for L1, atomically incrementing its refcount to 2
 * - Thread 1 releases L1 and atomically decrements its refcount to 1
 * - Thread 2 now acquires L1 and begins execution of protected code
 * - Thread 3 waits for L1, atomically incrementing its refcount to 2
 *
 *
 * The only way for incorrect simultaneous execution is if the lock
 * is deleted while a thread is waiting for it or holding it (i.e.,
 * any time after `lockVar.lock()` has been invoked but before `lockVar.unlock()`).
 * But now, when `lockVar.unlock()` is called, the refcount will always be >1,
 * so the lock will never be deleted.
 *
 * Memory leak is also impossible when properly used, because lock
 * is always deleted when refcount is 0.
 *
 * Also remains reentrant.
 *
 * Useful discussions here: https://stackoverflow.com/questions/41898355/lock-handler-for-arbitrary-keys
 */

class PerKeyLocks<K> {
  private final ConcurrentHashMap<K, Pair<Lock, Integer>> locks;

  public PerKeyLocks() {
    this.locks = new ConcurrentHashMap<>();
  }

  public void acquire(K key) {
    final Lock lock =
        locks
            .compute(
                key,
                (k, v) -> {
                  if (v == null) {
                    return Pair.of(new ReentrantLock(), 1);
                  }
                  return Pair.of(v.getLeft(), v.getRight() + 1);
                })
            .getLeft();

    lock.lock();
  }

  public void release(K key) {
    final Pair<Lock, Integer> lockAndRefCount = locks.get(key);

    locks.compute(
        key,
        (k, v) -> {
          if (v == null || v.getRight() == 1) {
            return null;
          }
          return Pair.of(v.getLeft(), v.getRight() - 1);
        });

    if (lockAndRefCount == null) {
      return;
    }
    final Lock lock = lockAndRefCount.getLeft();
    lock.unlock();
  }
}

public class SingleEventBatcher {
  private static final Logger logger = LoggerFactory.getLogger(SingleEventBatcher.class);

  private final AtomicBoolean isInitialized;
  private final AtomicInteger targetEventBatchSize;

  // From docs, unfortunately we shouldn't use core pool size 0 or keep alive times
  // with ScheduledThreadPoolExecutor, which means automatic shutdown is impossible.
  // https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ScheduledThreadPoolExecutor.html
  private static final int EXECUTOR_CORE_POOL_SIZE = 0;
  private final ScheduledThreadPoolExecutor allThreadsTimer;

  private final ConcurrentHashMap<Long, List<S3SingleExtractedEvent>> batchesByThread;
  private final ConcurrentHashMap<Long, ScheduledFuture<Void>> batchMaxAgeTasksByThread;
  private final PerKeyLocks<Long> batchRemovalLocksByThread;

  private final BatchEventProcessor batchEventProcessor;

  private AtomicBoolean isShutDown;

  public SingleEventBatcher(
      AtomicBoolean isInitialized,
      AtomicInteger targetEventBatchSize,
      BatchEventProcessor batchEventProcessor) {
    this.isInitialized = isInitialized;
    this.targetEventBatchSize = targetEventBatchSize;

    this.batchesByThread = new ConcurrentHashMap<>();
    this.batchMaxAgeTasksByThread = new ConcurrentHashMap<>();
    this.batchRemovalLocksByThread = new PerKeyLocks<>();
    this.allThreadsTimer = new ScheduledThreadPoolExecutor(EXECUTOR_CORE_POOL_SIZE);
    this.allThreadsTimer.setExecuteExistingDelayedTasksAfterShutdownPolicy(true);
    this.allThreadsTimer.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

    this.batchEventProcessor = batchEventProcessor;
    this.isShutDown = new AtomicBoolean(false);
  }

  public static final int DEFAULT_TARGET_BATCH_SIZE = 100;
  private static final int EVENT_MAX_AGE_MS = 2000;
  // `java -jar jol-cli-latest.jar internals -classpath lib/build/classes/java/main
  // org.dashdive.ImmutableS3SingleExtractedEvent`
  // Ballpark size of S3SingleExtractedEvent object:
  // - Contains 3 map fields (JOL gives total size 24 bytes)
  // - Error and telemetry maps may be larger, but assume they're
  //   quite rare (nonexistent if everything goes well)
  // - Maps could be one of any number of classes with variable size
  //   - JOL gives 48 bytes for base HashMap
  //   - Most entries are Strings, so at 24 bytes base size plus content;
  //     estimate liberally at 4 entries avg per map
  //   - Sample events have, liberally, 100-200 chars across all strings
  //     (2 bytes per char due to UTF-16) --> 400 bytes
  //
  // Total: 24 + (1 * (48 + 4 * (24 + 400))) = 1,768 bytes per event
  //
  // Let's say our maximum target mem usage per thread due to queue is 2 MB.
  // This is reasonable because JVM mem usage is usually at least 30-90 MB,
  // and we don't expect most clients to be running many concurrent threads.
  // This gives 2 MB / 1,768 bytes = >1,000 events per thread.
  // Source: https://spring.io/blog/2015/12/10/spring-boot-memory-performance
  private static final int HARD_MAX_QUEUE_LEN_PER_THREAD = 1000;

  public void shutDownAndFlush() {
    isShutDown.set(true);
    allThreadsTimer.shutdownNow();
    for (long threadId : batchesByThread.keySet()) {
      batchRemovalLocksByThread.acquire(threadId);
      try {
        removeAndSendBatch(threadId);
      } finally {
        batchRemovalLocksByThread.release(threadId);
      }
    }
  }

  // Returns whether enqueue was successful (for testing)
  public boolean queueEventForIngestion(final S3SingleExtractedEvent event) {
    if (isShutDown.get()) {
      logger.warn(
          "Attempted to queue event after shutdown; will be ignored [type={}]",
          event.dataPayload().get(S3EventFieldName.ACTION_TYPE));
      return false;
    }

    final long threadId = Thread.currentThread().threadId();
    batchRemovalLocksByThread.acquire(threadId);
    try {
      queueEventForIngestion_unlocked(threadId, event);
    } finally {
      batchRemovalLocksByThread.release(threadId);
    }
    return true;
  }

  private void queueEventForIngestion_unlocked(
      final long threadId, final S3SingleExtractedEvent event) {
    List<S3SingleExtractedEvent> currThreadBatch = batchesByThread.get(threadId);
    if (currThreadBatch == null) {
      currThreadBatch = new ArrayList<>();
      // Writing can cause contention if capacity is increased, but
      // this only happens when number of threads is 12 or more (due to default cap/load)
      // See: https://stackoverflow.com/a/54785722/14816795
      //
      // Writing won't cause contention most of the time since fine grained locking is used.
      // See: https://stackoverflow.com/a/10589196/14816795
      batchesByThread.put(threadId, currThreadBatch);
    }
    currThreadBatch.add(event);

    final int currBatchSize = currThreadBatch.size();
    final boolean shouldRespectTargetBatchSize = isInitialized.get();
    final boolean isBatchFull =
        (shouldRespectTargetBatchSize
                ? currBatchSize >= targetEventBatchSize.get()
                : currBatchSize >= DEFAULT_TARGET_BATCH_SIZE)
            || currBatchSize >= HARD_MAX_QUEUE_LEN_PER_THREAD;
    if (isBatchFull) {
      // Batch queue for this thread is about to be empty, so we won't need a scheduled
      // batch removal until at least one event is queued subsequently
      final ScheduledFuture<Void> currScheduledBatchRemoval =
          batchMaxAgeTasksByThread.get(threadId);
      if (currScheduledBatchRemoval != null) {
        currScheduledBatchRemoval.cancel(false);
      }
      removeAndSendBatch(threadId);
    } else {
      // If we're here, batch has at least one element, so we need to schedule a batch removal
      if (!batchMaxAgeTasksByThread.containsKey(threadId)) {
        final ScheduledFuture<Void> nextScheduledBatchRemoval =
            allThreadsTimer.schedule(
                () -> {
                  batchRemovalLocksByThread.acquire(threadId);
                  try {
                    removeAndSendBatch(threadId);
                    batchMaxAgeTasksByThread.remove(threadId);
                  } finally {
                    batchRemovalLocksByThread.release(threadId);
                  }
                  return null;
                },
                EVENT_MAX_AGE_MS,
                TimeUnit.MILLISECONDS);
        batchMaxAgeTasksByThread.put(threadId, nextScheduledBatchRemoval);
      }
    }
  }

  private void removeAndSendBatch(final long threadId) {
    final List<S3SingleExtractedEvent> batch = batchesByThread.get(threadId);
    if (batch == null) {
      return;
    }
    // The `enqueueOrDropBatch` function is expected to not be indefinitely blocking
    // and ideally takes very little time at all
    batchEventProcessor.enqueueOrDropBatch(batch);

    // Remove the ArrayList every time a batch is sent, so when the
    // client thread terminates, there isn't any cleanup to do
    batchesByThread.remove(threadId);
  }

  @VisibleForTesting
  public void _blockUntilShutdownComplete() throws InterruptedException {
    allThreadsTimer.awaitTermination(1, TimeUnit.DAYS);
  }
}
