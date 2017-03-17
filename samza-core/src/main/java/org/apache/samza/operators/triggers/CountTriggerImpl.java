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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation class for a {@link CountTrigger}
 */
public class CountTriggerImpl<M, WK> implements TriggerImpl<M, WK> {
  private static final Logger LOG = LoggerFactory.getLogger(CountTriggerImpl.class);

  private final long triggerCount;
  private final TriggerKey<WK> triggerKey;
  private long currentCount;
  private boolean shouldFire = false;

  public CountTriggerImpl(CountTrigger<M> triggerCount, TriggerKey<WK> triggerKey) {
    this.triggerCount = triggerCount.getCount();
    this.currentCount = 0;
    this.triggerKey = triggerKey;
  }

  public void onMessage(M message, TriggerScheduler<WK> context) {
    currentCount++;
    if (currentCount == triggerCount) {
      LOG.trace("count trigger fired for {}", message);
      shouldFire = true;
    }
  }

  @Override
  public void cancel() {
    //no-op
  }

  @Override
  public boolean shouldFire() {
    return shouldFire;
  }
}
