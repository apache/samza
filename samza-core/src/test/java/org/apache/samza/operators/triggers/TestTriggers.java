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
    final int numExpectedCallbacks = 5;
    final CountDownLatch latch = new CountDownLatch(numExpectedCallbacks);

    TriggerImpl.TriggerCallbackHandler handler = new TriggerImpl.TriggerCallbackHandler() {
      @Override
      public void onTrigger(TriggerImpl trigger, Object storeKey) {
        if (numCurrentCallbacks.incrementAndGet() <= numExpectedCallbacks) {
          //simulate a new message every duration.
          trigger.onMessage(null);
          latch.countDown();
        }
      }
    };

    Trigger repeatTrigger = Triggers.repeat(new TimeTrigger<>(Duration.ofMillis(200)));
    TriggerImpl<?> impl = TriggerImpls.createTriggerImpl(repeatTrigger, context, handler);
    impl.onMessage(null);
    latch.await();
  }

  @Test
  public void testAnyTrigger() throws Exception {
    TestTriggerContext context = new TestTriggerContext();

    // expect 2 callbacks, one for the count trigger and one for the timer trigger
    final int numExpectedCallbacks = 2;
    final int numMessages = 5;
    Trigger anyTrigger = Triggers.any(Triggers.count(numMessages), new TimeTrigger(Duration.ofMillis(10)));

    final CountDownLatch latch = new CountDownLatch(numExpectedCallbacks);
    final AtomicInteger numCurrentCallbacks = new AtomicInteger(0);

    TriggerImpl.TriggerCallbackHandler handler = new TriggerImpl.TriggerCallbackHandler() {
      @Override
      public void onTrigger(TriggerImpl impl, Object storeKey) {
        if (numCurrentCallbacks.incrementAndGet() <= numExpectedCallbacks) {
          latch.countDown();
        }
      }
    };
    TriggerImpl impl = TriggerImpls.createTriggerImpl(anyTrigger, context, handler);

    for (int i = 0; i < numMessages; i++) {
      impl.onMessage(null);
    }
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
      public void onTrigger(TriggerImpl impl, Object storeKey) {
        latch.countDown();
      }
    };

    TriggerImpl impl = TriggerImpls.createTriggerImpl(sinceLastMessage, context, handler);

    //pass in 5 messages in quick succession
    for (int i = 0; i < 5; i++) {
      impl.onMessage(null);
    }

    long beforeTime = System.currentTimeMillis();

    //Simulate a timeout for the specified session gap of 1 second.
    latch.await();
    long afterTime = System.currentTimeMillis();
    //Assert that the trigger fired after 1 second of inactivity.
    Assert.assertTrue(afterTime - beforeTime >= triggerDuration.toMillis());
  }

}
