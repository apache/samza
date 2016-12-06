package org.apache.samza.operators.windows.experimental;

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.windows.WindowOutput;

/**
 * A {@link Window} subdivides a {@link org.apache.samza.operators.MessageStream} into smaller finite chunks. Programmers
 * should use the API methods of {@link Windows} to specify their windowing functions.
 *
 * There are the following aspects to windowing in Samza:
 *
 * Default Trigger: Every {@link Window} has
 * a default {@link TriggersBuilder.Trigger} that specifies when to emit
 * its results.
 *
 * Early and Late Triggers: Users can choose to emit early, partial results speculatively by configuring an early trigger. Users
 * can choose to handle arrival of late data by configuring a late trigger. Refer to the {@link TriggersBuilder} APIs for
 * configuring early and late triggers.
 *
 * Aggregation: Every {@link Window} has an aggregation function to be applied on each element that returns the type of the value
 * stored in the window.
 *
 * Key Function: A {@link Window} can be keyed by a certain key in the input {@link MessageEnvelope}. For example, A common use-case is to perform
 * key based aggregations over a time window. When a key function is specified, the triggering behavior is per key per window.
 *
 *
 * @param <M> type of input {@link MessageEnvelope}.
 * @param <K> type of key to use for aggregation.
 * @param <WK> type of key in the window output.
 * @param <WV> type of value stored in the {@link Window}.
 * @param <WM> type of the {@link Window} result.
 */

public interface Window<M extends MessageEnvelope,K, WK, WV, WM extends WindowOutput<WK, WV>> {

  /**
   * Set the triggers for this {@link Window}
   *
   * @param wndTrigger trigger conditions set by the programmers
   * @return the {@link Window} function w/ the trigger {@code wndTrigger}
   */
  Window<M, K, WK, WV, WM> setTriggers(TriggersBuilder.Triggers wndTrigger);
}
