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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;


public class CompletableFutureUtil {

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
   * Blocks for a list of futures to complete.
   * @param futures list of futures to block for
   */
  public static <V, FV extends CompletionStage<V>> void joinAll(Collection<FV> futures) {
    CompletableFuture<Void> completeAllFutures =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    completeAllFutures.join(); // block and wait for all futures to complete.
  }
}
