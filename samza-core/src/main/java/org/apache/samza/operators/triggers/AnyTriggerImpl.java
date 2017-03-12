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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Implementation of an {@link AnyTrigger}
 *
 */
public class AnyTriggerImpl<M extends MessageEnvelope> implements TriggerImpl<M> {

  private final List<Trigger<M>> triggerList;

  private final Map<TriggerImpl<M>, Boolean> triggerImpls = new ConcurrentHashMap<>();
  private final Clock clock;
  private boolean shouldFire = false;

  public AnyTriggerImpl(AnyTrigger<M> anyTrigger, Clock clock) {
    this.triggerList = anyTrigger.getTriggers();
    this.clock = clock;
    for (Trigger<M> trigger : triggerList) {
      triggerImpls.put(TriggerImpls.createTriggerImpl(trigger, clock), false);
    }
  }

  @Override
  public void onMessage(M message, TriggerContext context) {
    if (!shouldFire) {
    System.out.println("inside anytrigger on message " + message.getKey() + " " + message.getMessage() + " " + this + " " + triggerImpls.size());

    for (TriggerImpl<M> impl : triggerImpls.keySet()) {
      impl.onMessage(message, context);
      if (impl.shouldFire()) {
        shouldFire = true;
        break;
      }
    }

    if (shouldFire) {
      cancel();
    }

    System.out.println("ended on message " + message.getKey() + " " + message.getMessage() + " " + this);
   }
  }

  public void cancel() {
    System.out.println("canceling anytrigger");
    for (Iterator<Map.Entry<TriggerImpl<M>, Boolean>> it = triggerImpls.entrySet().iterator(); it.hasNext(); ) {
      TriggerImpl<M> impl = it.next().getKey();
      impl.cancel();
      it.remove();
    }
  }

  @Override
  public boolean shouldFire() {
    for (TriggerImpl<M> impl : triggerImpls.keySet()) {
      if (impl.shouldFire()) {
        shouldFire = true;
      }
    }
    return shouldFire;
  }
}
