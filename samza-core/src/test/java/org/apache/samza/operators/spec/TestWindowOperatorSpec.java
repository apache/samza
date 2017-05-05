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

package org.apache.samza.operators.spec;

import org.apache.samza.operators.MessageStreamImpl;
import org.apache.samza.operators.triggers.Trigger;
import org.apache.samza.operators.triggers.Triggers;
import org.apache.samza.operators.windows.internal.WindowInternal;
import org.apache.samza.operators.windows.internal.WindowType;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

public class TestWindowOperatorSpec {
  @Test
  public void testTriggerIntervalWithNestedTimeTriggers() {
    Trigger defaultTrigger = Triggers.timeSinceFirstMessage(Duration.ofMillis(150));
    Trigger lateTrigger = Triggers.any(Triggers.count(6), Triggers.timeSinceFirstMessage(Duration.ofMillis(15)));
    Trigger earlyTrigger = Triggers.repeat(
        Triggers.any(Triggers.count(23),
            Triggers.timeSinceFirstMessage(Duration.ofMillis(15)),
            Triggers.any(Triggers.any(Triggers.count(6),
                Triggers.timeSinceFirstMessage(Duration.ofMillis(15)), Triggers.timeSinceFirstMessage(Duration.ofMillis(25)),
                Triggers.timeSinceLastMessage(Duration.ofMillis(15))))));

    WindowInternal window = new WindowInternal(defaultTrigger, null, null, null, null, WindowType.SESSION);
    window.setEarlyTrigger(earlyTrigger);
    window.setLateTrigger(lateTrigger);

    WindowOperatorSpec spec = new WindowOperatorSpec(window, new MessageStreamImpl(null), 0);
    Assert.assertEquals(spec.getTriggerMs(), 5);
  }

  @Test
  public void testTriggerIntervalWithSingleTimeTrigger() {
    Trigger defaultTrigger = Triggers.timeSinceFirstMessage(Duration.ofMillis(150));
    Trigger earlyTrigger = Triggers.repeat(Triggers.count(5));

    WindowInternal window = new WindowInternal(defaultTrigger, null, null, null, null, WindowType.SESSION);
    window.setEarlyTrigger(earlyTrigger);

    WindowOperatorSpec spec = new WindowOperatorSpec(window, new MessageStreamImpl(null), 0);
    Assert.assertEquals(spec.getTriggerMs(), 150);
  }
}
