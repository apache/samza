/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.container.disk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An implementation of {@link DiskSpaceMonitor} that polls for disk usage based on a specified
 * polling interval.
 * <p>
 * This class is thread-safe.
 */
public class PollingScanDiskSpaceMonitor implements DiskSpaceMonitor {
  private enum State { INIT, RUNNING, STOPPED }

  private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryImpl();
  private static final Logger log = LoggerFactory.getLogger(PollingScanDiskSpaceMonitor.class);

  // Note: we use this as a set where the value is always Boolean.TRUE.
  private final ConcurrentMap<Listener, Boolean> listenerSet = new ConcurrentHashMap<>();

  // Used to guard write access to state and listenerSet.
  private final Object lock = new Object();

  private final ScheduledExecutorService schedulerService =
      Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);
  private final Set<Path> watchPaths;
  private final long pollingIntervalMillis;

  private State state = State.INIT;

  /**
   * Returns the total size in bytes used by the specified paths. This function guarantees that it
   * will not double count overlapping portions of the path set. For example, with a trivial
   * overlap of /A and /A, it will count /A only once. It also handles other types of overlaps
   * similarly, such as counting /A/B only once given the paths /A and /A/B.
   * <p>
   * This function is exposed as package private to simplify testing various cases without involving
   * an executor. Alternatively this could have been pulled out to a utility class, but it would
   * unnecessarily pollute the global namespace.
   */
  static long getSpaceUsed(Set<Path> paths) {
    ArrayDeque<Path> pathStack = new ArrayDeque<>();

    for (Path path : paths) {
      pathStack.push(path);
    }

    // Track the directories we've visited to ensure we're not double counting. It would be
    // preferable to resolve overlap once at startup, but the problem is that the filesystem may
    // change over time and, in fact, at startup I found that the rocks DB store directory was not
    // created by the time the disk space monitor was started.
    Set<Path> visited = new HashSet<>();
    long totalBytes = 0;
    while (!pathStack.isEmpty()) {
      try {
        // We need to resolve to the real path to ensure that we don't inadvertently double count
        // due to different paths to the same directory (e.g. /A and /A/../A).
        Path current = pathStack.pop().toRealPath();

        if (visited.contains(current)) {
          continue;
        }
        visited.add(current);

        BasicFileAttributes currentAttrs = Files.readAttributes(current,
                                                                BasicFileAttributes.class);
        if (currentAttrs.isDirectory()) {
          try (DirectoryStream<Path> directoryListing = Files.newDirectoryStream(current)) {
            for (Path child : directoryListing) {
              pathStack.push(child);
            }
          }
        } else if (currentAttrs.isRegularFile()) {
          totalBytes += currentAttrs.size();
        }
      } catch (IOException e) {
        // If we can't stat the file, just ignore it. This can happen, for example, if we scan
        // a directory, but by the time we get to stat'ing the file it has been deleted (e.g.
        // due to compaction, rotation, etc.).
      }
    }

    return totalBytes;
  }

  /**
   * Creates a new disk space monitor that uses a periodic polling mechanism.
   *
   * @param watchPaths the set of paths to watch
   * @param pollingIntervalMillis the polling interval in milliseconds
   */
  public PollingScanDiskSpaceMonitor(Set<Path> watchPaths, long pollingIntervalMillis) {
    this.watchPaths = Collections.unmodifiableSet(new HashSet<>(watchPaths));
    this.pollingIntervalMillis = pollingIntervalMillis;
  }

  @Override
  public void start() {
    synchronized (lock) {
      switch (state) {
        case INIT:
          schedulerService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
              updateSample();
            }
          }, pollingIntervalMillis, pollingIntervalMillis, TimeUnit.MILLISECONDS);

          state = State.RUNNING;
          break;

        case RUNNING:
          // start is idempotent
          return;

        case STOPPED:
          throw new IllegalStateException("PollingScanDiskSpaceMonitor was stopped and cannot be restarted.");
      }
    }
  }

  @Override
  public void stop() {
    synchronized (lock) {
      // We could also wait for full termination of the scheduler service, but it is overkill for
      // our use case.
      schedulerService.shutdownNow();

      listenerSet.clear();
      state = State.STOPPED;
    }
  }

  @Override
  public boolean registerListener(Listener listener) {
    synchronized (lock) {
      if (state != State.STOPPED) {
        return listenerSet.putIfAbsent(listener, Boolean.TRUE) == Boolean.TRUE;
      }
    }
    return false;
  }

  /**
   * Wait until this service has shutdown. Returns true if shutdown occurred within the timeout
   * and false otherwise.
   * <p>
   * This is currently exposed at the package private level for tests only.
   */
  boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return schedulerService.awaitTermination(timeout, unit);
  }

  private void updateSample() {
    long totalBytes = getSpaceUsed(watchPaths);
    for (Listener listener : listenerSet.keySet()) {
      try {
        listener.onUpdate(totalBytes);
      } catch (Throwable e) {
        // catch an exception thrown by one listener so that it does not impact other listeners.
        log.error("Exception thrown by a listener ", e);
      }
    }
  }

  private static class ThreadFactoryImpl implements ThreadFactory  {
    private static final String PREFIX = "Samza-" + PollingScanDiskSpaceMonitor.class.getSimpleName() + "-";
    private static final AtomicInteger INSTANCE_NUM = new AtomicInteger();

    public Thread newThread(Runnable runnable) {
      return new Thread(runnable, PREFIX + INSTANCE_NUM.getAndIncrement());
    }
  }
}
