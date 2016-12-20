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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * API for creating {@link Trigger} instances to be used with a {@link org.apache.samza.operators.windows.Window}.
 *
 * <p> The below example groups an input into tumbling windows of 10s and emits early results periodically every 4s in
 * processing time, or for every 50 messages. It also specifies that window results are accumulating.
 *
 * <pre> {@code
 *   MessageStream<> windowedStream = stream.window(Windows.tumblingWindow(Duration.of(10, TimeUnit.SECONDS))
 *     .setEarlyTrigger(Triggers.repeat(Triggers.any(Triggers.count(50), Triggers.timeSinceFirstMessage(Duration.of(4, TimeUnit.SECONDS))))))
 *     .accumulateFiredPanes());
 * }</pre>
 *
 * @param <M> the type of input {@link MessageEnvelope}s in the {@link org.apache.samza.operators.MessageStream}
 * @param <K> the type of key in the {@link MessageEnvelope}
 * @param <V> the type of value in the {@link MessageEnvelope}
 */
@InterfaceStability.Unstable
public final class Triggers<M extends MessageEnvelope, K, V> {

  private Triggers() { }

  /**
   * Creates a {@link Trigger} that fires when the number of {@link MessageEnvelope}s in the pane
   * reaches the specified count.
   *
   * @param count the number of {@link MessageEnvelope}s to fire the trigger after
   * @return the created trigger
   */
  public static Trigger count(long count) {
    return new CountTrigger(count);
  }

  /**
   * Creates a trigger that fires after the specified duration has passed since the first {@link MessageEnvelope} in
   * the pane.
   *
   * @param duration the duration since the first element
   * @return the created trigger
   */
  public static Trigger timeSinceFirstMessage(Duration duration) {
    return new TimeSinceFirstMessageTrigger(duration);
  }

  /**
   * Creates a trigger that fires when there is no new {@link MessageEnvelope} for the specified duration in the pane.
   *
   * @param duration the duration since the last element
   * @return the created trigger
   */
  public static Trigger timeSinceLastMessage(Duration duration) {
    return new TimeSinceLastMessageTrigger(duration);
  }

  /**
   * Creates a trigger that fires when any of the provided triggers fire.
   *
   * @param <M> the type of input {@link MessageEnvelope} in the window
   * @param <K> the type of key in the {@link MessageEnvelope}
   * @param <V> the type of value in the {@link MessageEnvelope}
   * @param triggers the individual triggers
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
   * Repeats the provided trigger forever.
   *
   * <p>Creating a {@link RepeatingTrigger} from an {@link AnyTrigger} is equivalent to creating an {@link AnyTrigger} from
   * its individual {@link RepeatingTrigger}s.
   *
   * @param <M> the type of input {@link MessageEnvelope} in the window
   * @param <K> the type of key in {@link MessageEnvelope}
   * @param <V> the type of value in the {@link MessageEnvelope}
   * @param trigger the individual trigger to repeat
   * @return the created trigger
   */
  public static <M extends MessageEnvelope, K, V> Trigger repeat(Trigger<M, K, V> trigger) {
    return new RepeatingTrigger<>(trigger);
  }
}
