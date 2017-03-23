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

import org.apache.samza.operators.impl.TriggerKey;
import org.apache.samza.operators.impl.TriggerScheduler;
import org.apache.samza.util.Clock;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of an {@link AnyTrigger}
 */
public class AnyTriggerImpl<M, WK> implements TriggerImpl<M, WK> {

  private final List<Trigger<M>> triggers;

  private final List<TriggerImpl<M, WK>> triggerImpls = new ArrayList<>();
  private final Clock clock;
  private boolean shouldFire = false;

  public AnyTriggerImpl(AnyTrigger<M> anyTrigger, Clock clock, TriggerKey<WK> triggerKey) {
    this.triggers = anyTrigger.getTriggers();
    this.clock = clock;
    for (Trigger<M> trigger : triggers) {
      triggerImpls.add(TriggerImpls.createTriggerImpl(trigger, clock, triggerKey));
    }
  }

  @Override
  public void onMessage(M message, TriggerScheduler<WK> context) {
    for (TriggerImpl<M, WK> impl : triggerImpls) {
      impl.onMessage(message, context);
      if (impl.shouldFire()) {
        shouldFire = true;
        break;
      }
    }
    if (shouldFire) {
      cancel();
    }
  }

  public void cancel() {
    for (Iterator<TriggerImpl<M, WK>> it = triggerImpls.iterator(); it.hasNext(); ) {
      TriggerImpl<M, WK> impl = it.next();
      impl.cancel();
      it.remove();
    }
  }

  @Override
  public boolean shouldFire() {
    for (TriggerImpl<M, WK> impl : triggerImpls) {
      if (impl.shouldFire()) {
        shouldFire = true;
        break;
      }
    }
    return shouldFire;
  }
}
