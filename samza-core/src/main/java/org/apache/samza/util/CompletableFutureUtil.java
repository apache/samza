package org.apache.samza.util;

import java.util.Collection;
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
   * Blocks for a list of futures to complete.
   * @param futures list of futures to block for
   */
  public static <V, FV extends CompletionStage<V>> void blockAndCompleteAll(Collection<FV> futures) {
    // ToDo change whenComplete, replace with async version. Check docs.
    // TODO HIGH shesharm: do we need the whenComplete here?
    CompletableFuture<Void> completeAllFutures =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));

    completeAllFutures.join(); // block and wait for all futures to complete.
  }
}
