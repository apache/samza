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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.Function;
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
      fvs.add(CompletableFuture.allOf(futureCollection.toArray(new CompletableFuture[0])));
    }

    return CompletableFuture.allOf(fvs.toArray(new CompletableFuture[0]));
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

  // https://stackoverflow.com/questions/40485398/retry-logic-with-completablefuture
  // TODO BLOCKER pmaheshw make retries configurable at all call sites
  public static <T> CompletableFuture<T> executeAsyncWithRetries(
      String name, Supplier<CompletableFuture<T>> action,
      Executor executor, int maxRetries) {
    return action.get()
        .thenApplyAsync(CompletableFuture::completedFuture, executor)
        .exceptionally(t -> {
          LOG.warn("Initial action: {} completed with error. Will retry {} times. First error: ",
              name, maxRetries, t);
          return retryAsync(name, action, executor, t, maxRetries, 0);
        }) // happens synchronously on the thread for previous stage
        .thenComposeAsync(Function.identity(), executor);
  }

  private static <T> CompletableFuture<T> retryAsync(
      String name, Supplier<CompletableFuture<T>> action, Executor executor,
      Throwable firstError, int maxRetries, int currentAttempt) {
    if (currentAttempt >= maxRetries) return failedFuture(firstError);
    return action.get()
        .thenApplyAsync(CompletableFuture::completedFuture, executor)
        .exceptionally(t -> {
          LOG.warn("Previous action: {} completed with error. Retry attempt {} of {}. Current error: ",
              name, currentAttempt, maxRetries, t);
          firstError.addSuppressed(t);
          return retryAsync(name, action, executor, firstError, maxRetries, currentAttempt + 1);
        })
        .thenComposeAsync(Function.identity(), executor);
  }

  public static <T> CompletableFuture<T> failedFuture(Throwable t) {
    final CompletableFuture<T> cf = new CompletableFuture<>();
    cf.completeExceptionally(t);
    return cf;
  }
}
