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

import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.util.Clock;

/**
 * Implementation class for a {@link RepeatingTrigger}
 */
public class RepeatingTriggerImpl<M> implements TriggerImpl<M> {

  private final Trigger<M> repeatingTrigger;
  private final Clock clock;

  private TriggerImpl<M> currentTriggerImpl;
  private boolean shouldFire = false;

  public RepeatingTriggerImpl(RepeatingTrigger<M> repeatingTrigger, Clock clock) {
    this.repeatingTrigger = repeatingTrigger.getTrigger();
    this.clock = clock;
    this.currentTriggerImpl = TriggerImpls.createTriggerImpl(this.repeatingTrigger, clock);
  }

  @Override
  public void onMessage(M message, TriggerContext context) {
    currentTriggerImpl.onMessage(message, context);
  }

  @Override
  public void cancel() {
    currentTriggerImpl.cancel();
  }

  @Override
  public void clear() {
    currentTriggerImpl.cancel();
    currentTriggerImpl = TriggerImpls.createTriggerImpl(repeatingTrigger, clock);

  }

  @Override
  public boolean shouldFire() {
    return currentTriggerImpl.shouldFire();
  }
}
