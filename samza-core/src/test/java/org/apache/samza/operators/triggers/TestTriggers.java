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

import junit.framework.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class TestTriggers {
  @Test
  public void testRepeatingTimerTriggers() throws Exception {
    TestTriggerContext context = new TestTriggerContext();
    final AtomicInteger numCurrentCallbacks = new AtomicInteger(0);
    final int numExpectedCallbacks = 2;
    final CountDownLatch latch = new CountDownLatch(numExpectedCallbacks);
    final int timeDurationMs = 200;
    TriggerImpl.TriggerCallbackHandler handler = new TriggerImpl.TriggerCallbackHandler() {
      @Override
      public void onTrigger() {
        if (numCurrentCallbacks.incrementAndGet() <= numExpectedCallbacks) {
          //simulate a new message every duration.
          latch.countDown();
        }
      }
    };

    Trigger repeatTrigger = Triggers.repeat(new TimeTrigger<>(Duration.ofMillis(timeDurationMs)));
    TriggerImpl<?> impl = TriggerImpls.createTriggerImpl(repeatTrigger);
    impl.onMessage(null, context, handler);
    Thread.sleep(timeDurationMs * 2);
    impl.onMessage(null, context, handler);
    latch.await();
  }


  @Test
  public void testAnyTrigger() throws Exception {
    TestTriggerContext context = new TestTriggerContext();

    final int numExpectedCallbacks = 1;
    final int numMessages = 5;
    Trigger anyTrigger = Triggers.any(Triggers.count(numMessages), new TimeTrigger(Duration.ofMillis(10)));

    final CountDownLatch latch = new CountDownLatch(numExpectedCallbacks);
    final AtomicInteger numCurrentCallbacks = new AtomicInteger(0);

    TriggerImpl.TriggerCallbackHandler handler = new TriggerImpl.TriggerCallbackHandler() {
      @Override
      public void onTrigger() {
          latch.countDown();
      }
    };
    TriggerImpl impl = TriggerImpls.createTriggerImpl(anyTrigger);

    impl.onMessage(null, context, handler);
    latch.await();
  }

  @Test
  public void testTimeSinceLastMessageTrigger() throws Exception {
    Duration triggerDuration = Duration.ofSeconds(1);
    Trigger sinceLastMessage = Triggers.timeSinceLastMessage(triggerDuration);
    TestTriggerContext context = new TestTriggerContext();
    CountDownLatch latch = new CountDownLatch(1);

    TriggerImpl.TriggerCallbackHandler handler = new TriggerImpl.TriggerCallbackHandler() {
      @Override
      public void onTrigger() {
        latch.countDown();
      }
    };

    TriggerImpl impl = TriggerImpls.createTriggerImpl(sinceLastMessage);

    //pass in 5 messages in quick succession
    for (int i = 0; i < 5; i++) {
      impl.onMessage(null, context, handler);
    }

    long beforeTime = System.currentTimeMillis();

    //Simulate a timeout for the specified session gap of 1 second.
    latch.await();
    long afterTime = System.currentTimeMillis();
    //Assert that the trigger fired after 1 second of inactivity.
    Assert.assertTrue(afterTime - beforeTime >= triggerDuration.toMillis());
  }

}
