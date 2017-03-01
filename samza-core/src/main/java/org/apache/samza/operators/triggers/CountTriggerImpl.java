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
 * Implementation class for a {@link CountTrigger}
 */
public class CountTriggerImpl<M extends MessageEnvelope> extends TriggerImpl<M> {

  private final long triggerCount;
  private long currentCount;

  public CountTriggerImpl(CountTrigger<M> triggerCount, TriggerContext context, TriggerCallbackHandler handler) {
    super(context, handler);
    this.triggerCount = triggerCount.getCount();
    this.currentCount = 0;
  }

  public void onMessage(M message) {
    currentCount++;
    if (currentCount == triggerCount) {
      System.out.println("count trigger fired." + this + " " + this.context.getWindowKey());
      handler.onTrigger(this, context.getWindowKey());
    }
  }

  @Override
  public void onCancel() {
    //no-op
  }
}
