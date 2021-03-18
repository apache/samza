package org.apache.samza.util;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.SamzaException;


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
  public static <K, V> CompletableFuture<Map<K, V>> toFutureOfMap(Map<K, CompletableFuture<V>> ketyToValueFutureMap) {
    return CompletableFuture
        .allOf(ketyToValueFutureMap.values().toArray(new CompletableFuture[0]))
        .thenApply(v -> ketyToValueFutureMap.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().join())));
  }

  /**
   * Block and complete a list of futures or throw exception if any of the futures fail
   * @param completionStages list of futures to complete
   */
  public static void blockAndCompleteAll(List<CompletionStage<Void>> completionStages) {
    // ToDo change whenComplete, replace with async version. Check docs.
    // TODO HIGH shesharm: do we need the whenComplete here?
    CompletableFuture<Void> completeAllFutures =
        CompletableFuture.allOf(completionStages.toArray(new CompletableFuture[0]));

    completeAllFutures.join(); // block and wait for all futures to complete.
  }
}
