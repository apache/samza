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

package org.apache.samza.util;

import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FutureUtil {
  private static final Logger LOG = LoggerFactory.getLogger(FutureUtil.class);

  /**
   * Returns a future that completes when all the futures in the provided collections of futures are complete.
   * @param futureCollections collections of futures to complete before the returned future is complete
   */
  @SafeVarargs
  public static CompletableFuture<Void> allOf(Collection<? extends CompletionStage<?>>... futureCollections) {
    List<CompletableFuture<Void>> fvs = new ArrayList<>();
    for (Collection<? extends CompletionStage<?>> futureCollection : futureCollections) {
      if (!futureCollection.isEmpty()) {
        fvs.add(CompletableFuture.allOf(futureCollection.toArray(new CompletableFuture[0])));
      }
    }

    return CompletableFuture.allOf(fvs.toArray(new CompletableFuture[0]));
  }

  /**
   * Returns a future that completes when all the futures futures are complete.
   * Returned future completes exceptionally if any future complete with a non-ignored error.
   */
  public static CompletableFuture<Void> allOf(Predicate<Throwable> ignoreError, CompletableFuture<?>... futures) {
    CompletableFuture<Void> allFuture = CompletableFuture.allOf(futures);
    return allFuture.handle((aVoid, t) -> {
      for (CompletableFuture<?> future : futures) {
        try {
          future.join();
        } catch (Throwable th) {
          if (ignoreError.test(th)) {
            // continue
          } else {
            throw th;
          }
        }
      }
      return null;
    });
  }

  /**
   * Helper method to convert: {@code Pair<CompletableFuture<L>, CompletableFuture<R>>}
   * to:                       {@code CompletableFuture<Pair<L, R>>}
   *
   * Returns a future that completes when both futures complete.
   * Returned future completes exceptionally if either of the futures complete exceptionally.
   */
  public static <L, R> CompletableFuture<Pair<L, R>> toFutureOfPair(
      Pair<CompletableFuture<L>, CompletableFuture<R>> pairOfFutures) {
    return CompletableFuture
        .allOf(pairOfFutures.getLeft(), pairOfFutures.getRight())
        .thenApply(v -> Pair.of(pairOfFutures.getLeft().join(), pairOfFutures.getRight().join()));
  }

  /**
   * Helper method to convert: {@code Map<K, CompletableFuture<V>>}
   * to:                       {@code CompletableFuture<Map<K, V>>}
   *
   * Returns a future that completes when all value futures complete.
   * Returned future completes exceptionally if any of the value futures complete exceptionally.
   */
  public static <K, V> CompletableFuture<Map<K, V>> toFutureOfMap(Map<K, CompletableFuture<V>> keyToValueFutures) {
    return CompletableFuture
        .allOf(keyToValueFutures.values().toArray(new CompletableFuture[0]))
        .thenApply(v -> keyToValueFutures.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().join())));
  }

  /**
   * Helper method to convert: {@code Map<K, CompletableFuture<V>>}
   * to:                       {@code CompletableFuture<Map<K, V>>}
   *
   * Returns a future that completes with successful map entries, skipping any entries with ignored errors,
   * when all value futures complete.
   * Returned future completes exceptionally if any of the futures complete with a non-ignored error.
   */
  public static <K, V> CompletableFuture<Map<K, V>> toFutureOfMap(
      Predicate<Throwable> ignoreError, Map<K, CompletableFuture<V>> keyToValueFutures) {
    CompletableFuture<Void> allEntriesFuture =
        CompletableFuture.allOf(keyToValueFutures.values().toArray(new CompletableFuture[]{}));

    return allEntriesFuture.handle((aVoid, t) -> {
      Map<K, V> successfulResults = new HashMap<>();
      for (Map.Entry<K, CompletableFuture<V>> entry : keyToValueFutures.entrySet()) {
        K key = entry.getKey();
        try {
          V value = entry.getValue().join();
          successfulResults.put(key, value);
        } catch (Throwable th) {
          if (ignoreError.test(th)) {
            // else ignore and continue
            LOG.warn("Ignoring value future completion error for key: {}", key, th);
          } else {
            throw th;
          }
        }
      }
      return successfulResults;
    });
  }

  public static <T> CompletableFuture<T> executeAsyncWithRetries(String opName,
      Supplier<? extends CompletionStage<T>> action,
      Predicate<? extends Throwable> abortRetries,
      ExecutorService executor) {
    Duration maxDuration = Duration.ofMinutes(1);

    RetryPolicy<Object> retryPolicy = new RetryPolicy<>()
        .withBackoff(100, 10000, ChronoUnit.MILLIS)
        .withMaxDuration(maxDuration)
        .abortOn(abortRetries) // stop retrying if predicate returns true
        .onRetry(e -> LOG.warn("Action: {} attempt: {} completed with error {} after start. Retrying up to {}.",
            opName, e.getAttemptCount(), e.getElapsedTime(), maxDuration, e.getLastFailure()));

    return Failsafe.with(retryPolicy).with(executor).getStageAsync(action::get);
  }

  public static <T> CompletableFuture<T> failedFuture(Throwable t) {
    final CompletableFuture<T> cf = new CompletableFuture<>();
    cf.completeExceptionally(t);
    return cf;
  }

  /**
   * Removes wrapper exceptions of the provided type from the provided throwable and returns the first cause
   * that does not match the wrapper type. Useful for unwrapping CompletionException / SamzaException
   * in stack traces and getting to the underlying cause.
   *
   * Returns null if provided Throwable is null or if there is no cause of non-wrapper type in the stack.
   */
  public static <T extends Throwable> Throwable unwrapExceptions(Class<? extends Throwable> wrapperClassToUnwrap, T t) {
    if (t == null) return null;
    if (wrapperClassToUnwrap == null) return t;

    Throwable originalException = t;
    while (wrapperClassToUnwrap.isAssignableFrom(originalException.getClass()) &&
        originalException.getCause() != null) {
      originalException = originalException.getCause();
    }

    // can still be the wrapper class if no other cause was found.
    if (wrapperClassToUnwrap.isAssignableFrom(originalException.getClass())) {
      return null;
    } else {
      return originalException;
    }
  }
}
