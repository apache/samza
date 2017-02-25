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
 *     .setAccumulationMode(AccumulationMode.ACCUMULATING));
 * }</pre>
 *
 */
@InterfaceStability.Unstable
public final class Triggers {

  private Triggers() { }

  /**
   * Creates a {@link Trigger} that fires when the number of messages in the pane
   * reaches the specified count.
   *
   * @param count the number of messages to fire the trigger after
   * @param <M> the type of input message in the window
   * @return the created trigger
   */
  public static <M> Trigger<M> count(long count) {
    return new CountTrigger<M>(count);
  }

  /**
   * Creates a trigger that fires after the specified duration has passed since the first message in
   * the pane.
   *
   * @param duration the duration since the first element
   * @param <M> the type of input message in the window
   * @return the created trigger
   */
  public static <M> Trigger<M> timeSinceFirstMessage(Duration duration) {
    return new TimeSinceFirstMessageTrigger<M>(duration);
  }

  /**
   * Creates a trigger that fires when there is no new message for the specified duration in the pane.
   *
   * @param duration the duration since the last element
   * @param <M> the type of input message in the window
   * @return the created trigger
   */
  public static <M> Trigger<M> timeSinceLastMessage(Duration duration) {
    return new TimeSinceLastMessageTrigger<M>(duration);
  }

  /**
   * Creates a trigger that fires when any of the provided triggers fire.
   *
   * @param triggers the individual triggers
   * @param <M> the type of input message in the window
   * @return the created trigger
   */
  public static <M> Trigger<M> any(Trigger<M>... triggers) {
    List<Trigger<M>> triggerList = new ArrayList<>();
    for (Trigger trigger : triggers) {
      triggerList.add(trigger);
    }
    return new AnyTrigger<M>(Collections.unmodifiableList(triggerList));
  }

  /**
   * Repeats the provided trigger forever.
   *
   * <p>Creating a {@link RepeatingTrigger} from an {@link AnyTrigger} is equivalent to creating an {@link AnyTrigger} from
   * its individual {@link RepeatingTrigger}s.
   *
   * @param trigger the individual trigger to repeat
   * @param <M> the type of input message in the window
   * @return the created trigger
   */
  public static <M> Trigger<M> repeat(Trigger<M> trigger) {
    return new RepeatingTrigger<>(trigger);
  }
}
