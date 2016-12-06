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
package org.apache.samza.operators.triggers;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.operators.data.MessageEnvelope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * API for specifying {@link Trigger}s for a {@link org.apache.samza.operators.windows.WindowFunction}.
 *
 * <p> The below example windows an input into tumbling windows of 10s, and emits early results periodically every 4s in
 * processing time, or for every 50 messages. It also specifies that window results are accumulating.
 *
 * <pre> {@code
 * MessageStream<> windowedStream = stream.window(
 *   Windows.tumblingWindow(Duration.of(10, TimeUnit.SECONDS))
 *     .setEarlyTrigger(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.of(4, TimeUnit.SECONDS))))))
 *     .accumulateFiredPanes());
 *   }
 *  </pre>
 *
 * @param <M> type of input {@link MessageEnvelope} in the window
 * @param <K> type of key in {@link MessageEnvelope}
 * @param <V> type of value in the {@link MessageEnvelope}
 *
 */
@InterfaceStability.Unstable
public final class Triggers<M extends MessageEnvelope, K, V> {

  public static Trigger count(long count) {
    return new CountTrigger(count);
  }

  /**
   * Creates a trigger that emits results after a processing time delay from the first element in the pane.
   *
   * @param gap the gap period since the first element
   * @return the created trigger
   */
  public static Trigger timeSinceFirstMessage(Duration gap) {
    return new TimeSinceFirstMessageTrigger(new TimeTrigger(gap));
  }

  /**
   * Creates a trigger that emits results after a processing time delay from the last element in the pane.
   *
   * @param gap the gap period since the last element
   * @return the created trigger
   */
  public static Trigger timeSinceLastMessage(Duration gap) {
    return new TimeSinceLastMessageTrigger(new TimeTrigger(gap));
  }

  /**
   * Creates a trigger that is the disjunction of the individual triggers.
   *
   * @param <M> type of input {@link MessageEnvelope} in the window
   * @param <K> type of key in {@link MessageEnvelope}
   * @param <V> type of value in the {@link MessageEnvelope}
   * @param triggers the individual triggers
   *
   * @return the created trigger
   */
  public static <M extends MessageEnvelope, K, V> Trigger any(Trigger<M, K, V>... triggers) {
    List<Trigger> triggerList = new ArrayList<>();
    for (Trigger trigger : triggers) {
      triggerList.add(trigger);
    }
    return new AnyTrigger(Collections.unmodifiableList(triggerList));
  }

  /**
   * Repeats a trigger forever. Creating a {@link RepeatTrigger} from an {@link AnyTrigger} is equivalent to
   * creating an {@link AnyTrigger} from its individual {@link RepeatTrigger}s.
   *
   * @param <M> type of input {@link MessageEnvelope} in the window
   * @param <K> type of key in {@link MessageEnvelope}
   * @param <V> type of value in the {@link MessageEnvelope}
   * @param trigger the individual trigger to repeat
   *
   * @return the created trigger
   */
  public static <M extends MessageEnvelope, K, V> Trigger repeat(Trigger<M, K, V> trigger) {
    return new RepeatTrigger<>(trigger);
  }
}
