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

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * An implementation of the {@link TriggerContext} that uses a {@link Timer} to schedule triggers.
 */
public class TestTriggerContext implements TriggerContext {

  private final Timer timer = new Timer();
  Map<Cancellable, TimerTask> map = new HashMap<>();

  @Override
  public Cancellable scheduleCallback(Runnable runnable, long callbackTimeMs) {
    TimerTask task = new TimerTask() {
      @Override
      public void run() {
        runnable.run();
      }
    };

    Cancellable impl = new CancellableImpl();
    map.put(impl, task);
    Long delay = callbackTimeMs - System.currentTimeMillis();
    if (delay < 0) {
      delay = 0L;
    }

    timer.schedule(task, delay);
    return impl;
  }

  @Override
  public Object getWindowKey() {
    return null;
  }

  private class CancellableImpl implements Cancellable {
    @Override
    public boolean cancel() {
      return map.get(this).cancel();
    }
  }
}
