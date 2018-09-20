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
package org.apache.samza.executors;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


/**
 * This class supports submitting {@link Runnable} tasks with an ordering key, so that tasks submitted with the
 * same key will always be executed in order, but tasks across different keys can be executed in parallel and out of
 * order.
 * Ordering is achieved by hashing the key objects to threads by their {@link #hashCode()} method.
 * Ordering is guaranteed only when using the {@link #submitOrdered(Object, Runnable)} method. None of the
 * {@link #submit} and {@link #execute(Runnable)} method(s) guarantee the ordering semantics.
 */
public class KeyBasedExecutorService extends AbstractExecutorService {
  final String threadPoolNamePrefix;
  final ExecutorService[] executors;
  final Random rand = new Random();
  final int numThreads;

  public KeyBasedExecutorService(int numThreads) {
    this("KeyBasedExecutor", numThreads);
  }

  /**
   * Constructs an instance of a KeyBasedExecutorService that manages the underlying threads
   *
   * @param threadPoolNamePrefix String identifier for this ExecutorService. It forms the prefix for each of the
   *                             underlying thread pool executors
   * @param numThreads Total number of threads required, mainly dependent on the key set size and the degree of
   *                   parallelism. Highest level of parallelism can be achieved by setting the
   *                   number of threads = key set size.
   * @throws IllegalArgumentException if numThreads {@literal <}= 0
   */
  public KeyBasedExecutorService(String threadPoolNamePrefix,
      int numThreads) {
    if (numThreads <= 0) {
      throw new IllegalArgumentException("numThreads has to be greater than 0 in KeyBasedExecutor!");
    }
    this.numThreads = numThreads;
    this.threadPoolNamePrefix = threadPoolNamePrefix;
    this.executors = new ExecutorService[numThreads];
    final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();

    for (int i = 0; i < numThreads; i++) {
      final ExecutorService threadPoolExecutorPerQueue = Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder()
              .setThreadFactory(defaultThreadFactory)
              .setNameFormat(this.threadPoolNamePrefix + "-" + i + "-%d")
              .build()
      );
      executors[i] = threadPoolExecutorPerQueue;
    }
  }

  protected ExecutorService chooseRandomExecutor() {
    if (executors.length == 1) {
      return executors[0];
    }
    return executors[rand.nextInt(executors.length)];
  }

  protected ExecutorService chooseExecutor(Object object) {
    if (executors.length == 1) {
      return executors[0];
    }
    return executors[signSafeMod(object.hashCode(), executors.length)];
  }

  private static int signSafeMod(long dividend, int divisor) {
    int mod = (int) (dividend % divisor);
    if (mod < 0) {
      mod += divisor;
    }
    return mod;
  }

  @Override
  public void shutdown() {
    for (int i = 0; i < executors.length; i++) {
      executors[i].shutdown();
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    List<Runnable> unexecutedRunnables = new ArrayList<>();
    for (int i = 0; i < executors.length; i++) {
      List<Runnable> unexecutedRunnablesPerQueue = executors[i].shutdownNow();
      if (unexecutedRunnablesPerQueue != null && unexecutedRunnablesPerQueue.size() > 0) {
        unexecutedRunnables.addAll(unexecutedRunnablesPerQueue);
      }
    }
    return unexecutedRunnables;
  }

  @Override
  public boolean isShutdown() {
    boolean ret = true;
    for (int i = 0; i < executors.length; i++) {
      ret = ret && executors[i].isShutdown();
    }
    return ret;
  }

  @Override
  public boolean isTerminated() {
    boolean ret = true;
    for (int i = 0; i < executors.length; i++) {
      ret = ret && executors[i].isTerminated();
    }
    return ret;
  }

  /**
   * Awaits termination of each of the underlying threads
   *
   * Note: This can potentially block longer than the given timeout, since the timeout applies for each of the
   * underlying threads.
   *
   * @param timeout time to wait for each thread to terminate
   * @param unit unit of time for specifying timeout
   * @return Returns True, if all threads terminate successfully within their timeout. False, otherwise.
   * @throws InterruptedException thrown when the current executing thread is interrupted
   */
  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    boolean ret = true;
    for (int i = 0; i < executors.length; i++) {
      ret = ret && executors[i].awaitTermination(timeout, unit);
    }
    return ret;
  }

  public Future<?> submitOrdered(Object key, Runnable task) {
    return chooseExecutor(key).submit(task);
  }

  /**
   * Executes the given {@link Runnable} task in a randomly chosen thread-pool
   * @param command An instance of the {@link Runnable} task
   */
  @Override
  public void execute(Runnable command) {
    chooseRandomExecutor().execute(command);
  }
}
