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
   * Helper method to convert: {@code Pair<CompletableFuture<L>, CompletableFuture<R>>}
   * to:                       {@code CompletableFuture<Pair<L, R>>}
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
   */
  public static <K, V> CompletableFuture<Map<K, V>> toFutureOfMap(Map<K, CompletableFuture<V>> keyToValueFutures) {
    return CompletableFuture
        .allOf(keyToValueFutures.values().toArray(new CompletableFuture[0]))
        .thenApply(v -> keyToValueFutures.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().join())));
  }

  /**
   * Helper method to convert: {@code List<CompletableFuture<E>>}
   * to:                       {@code CompletableFuture<List<E>>}
   */
  public static <E> CompletableFuture<List<E>> toFutureOfList(List<CompletableFuture<E>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }

  /**
   * Returns a future that completes when all the futures in the provided collections of futures are complete.
   * @param futureCollections collections of futures to complete before the returned future is complete
   */
  @SafeVarargs
  public static <V, FV extends CompletionStage<V>> CompletableFuture<Void> allOf(Collection<FV>... futureCollections) {
    List<CompletableFuture<Void>> fvs = new ArrayList<>();
    for (Collection<FV> futureCollection : futureCollections) {
      if (!futureCollection.isEmpty()) {
        fvs.add(CompletableFuture.allOf(
            futureCollection.toArray(new CompletableFuture[0])));
      }
    }

    return CompletableFuture.allOf(fvs.toArray(new CompletableFuture[0]));
  }

  public static CompletableFuture<Void> allOf(Predicate<Throwable> ignoreError, CompletableFuture<?>... futures) {
    CompletableFuture<Void> allFuture = CompletableFuture.allOf(futures);
    CompletableFuture<Void> resultFuture = new CompletableFuture<>();
    allFuture.whenComplete((aVoid, t) -> {
      if (t == null || ignoreError.test(t)) {
        resultFuture.complete(null);
      } else {
        resultFuture.completeExceptionally(t);
      }
    });
    return resultFuture;
  }

  /**
   * Blocks for a list of futures to complete.
   * @param futures list of futures to block for
   */
  public static <V, FV extends CompletionStage<V>> void joinAll(Collection<FV> futures) {
    CompletableFuture<Void> completeAllFutures =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    completeAllFutures.join(); // block and wait for all futures to complete.
  }

  // TODO BLOCKER shesharm add unit tests
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

  // Nullable
  public static <T extends Throwable> Throwable unwrapExceptions(Class<? extends Throwable> wrapperClassToUnwrap, T t) {
    if (t == null) return null;

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
