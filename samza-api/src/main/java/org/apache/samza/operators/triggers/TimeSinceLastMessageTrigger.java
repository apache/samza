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

import java.time.Duration;

/*
 * A {@link Trigger} that fires when there are no new {@link MessageEnvelope}s in the window pane for the specified duration.
 */
public class TimeSinceLastMessageTrigger<M> implements Trigger {

  private final Duration duration;
  private final DurationCharacteristic characteristic = DurationCharacteristic.PROCESSING_TIME;

  TimeSinceLastMessageTrigger(Duration duration) {
    this.duration = duration;
  }

  public Duration getDuration() {
    return duration;
  }

  public DurationCharacteristic getCharacteristic() {
    return characteristic;
  }
}
