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
public class RepeatingTriggerImpl<M extends MessageEnvelope> implements TriggerImpl<M> {

  private final Trigger<M> underlyingTrigger;
  private final Clock clock;

  private TriggerImpl<M> underlyingTriggerImpl;

  public RepeatingTriggerImpl(RepeatingTrigger<M> repeatingTrigger, Clock clock) {
    this.underlyingTrigger = repeatingTrigger.getTrigger();
    this.clock = clock;
    this.underlyingTriggerImpl = TriggerImpls.createTriggerImpl(underlyingTrigger, clock);
  }

  private TriggerCallbackHandler createNewHandler(TriggerCallbackHandler handler) {
    return new TriggerCallbackHandler() {
      @Override
      public void onTrigger() {
          //re-schedule the underlying trigger for execution again.
        System.out.println("canceling repeat trigger");
          cancel();
          underlyingTriggerImpl = TriggerImpls.createTriggerImpl(underlyingTrigger, clock);
          handler.onTrigger();
        System.out.println("canceling repeat trigger end");
      }
    };
  }

  @Override
  public void onMessage(M message, TriggerContext context, TriggerCallbackHandler handler) {
    System.out.println("inside repeating trigger onmessage" + message.getKey() + " " + message.getMessage());
    underlyingTriggerImpl.onMessage(message, context, createNewHandler(handler));
  }

  @Override
  public void cancel() {
    underlyingTriggerImpl.cancel();
  }
}
