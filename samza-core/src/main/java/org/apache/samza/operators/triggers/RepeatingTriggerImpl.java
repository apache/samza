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

/**
 * Implementation class for a {@link RepeatingTrigger}
 */
public class RepeatingTriggerImpl<M extends MessageEnvelope> extends TriggerImpl<M> {

  private final Trigger underlyingTrigger;

  private TriggerImpl underlyingTriggerImpl;

  private boolean cancelled = false;

  public RepeatingTriggerImpl(RepeatingTrigger<M> repeatingTrigger, TriggerContext tContext, TriggerCallbackHandler handler) {
    super(tContext, handler);
    this.underlyingTrigger = repeatingTrigger.getTrigger();

    if (!cancelled) {
      this.underlyingTriggerImpl = TriggerImpls.createTriggerImpl(underlyingTrigger, context, createNewCallback());
    }
  }

  private TriggerCallbackHandler createNewCallback() {
    return new TriggerCallbackHandler() {
      @Override
      public void onTrigger(TriggerImpl impl, Object storeKey) {
        underlyingTriggerImpl = TriggerImpls.createTriggerImpl(underlyingTrigger, context, createNewCallback());
        handler.onTrigger(RepeatingTriggerImpl.this, storeKey);
      }
    };
  }

  @Override
  public void onMessage(M message) {
    underlyingTriggerImpl.onMessage(message);
  }

  @Override
  public void onCancel() {
    underlyingTriggerImpl.onCancel();
    cancelled = true;
  }
}
