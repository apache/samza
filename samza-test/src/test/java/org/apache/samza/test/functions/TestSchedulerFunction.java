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

package org.apache.samza.test.functions;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.Scheduler;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.functions.ScheduledFunction;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestSchedulerFunction {
  static AtomicBoolean timerFired = new AtomicBoolean(false);

  @Test
  public void testImmediateTimer() {
    final InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    final InMemoryInputDescriptor<Integer> imid = isd.getInputDescriptor("test-input", new IntegerSerde());

    StreamApplication app = new StreamApplication() {
      @Override
      public void describe(StreamApplicationDescriptor appDescriptor) {

        appDescriptor.getInputStream(imid)
            .map(new TestFunction());
      }
    };

    TestRunner
        .of(app)
        .addInputStream(imid, Arrays.asList(1, 2, 3, 4, 5))
        .run(Duration.ofSeconds(1));

    assertTrue(timerFired.get());
  }

  private static class TestFunction implements MapFunction<Integer, Void>, ScheduledFunction<String, Void> {

    @Override
    public Void apply(Integer message) {
      try {
        // This is to avoid the container shutdown before the timer fired
        Thread.sleep(1);
      } catch (Exception e) {
      }
      return null;
    }

    @Override
    public void schedule(Scheduler<String> scheduler) {
      scheduler.schedule("haha", System.currentTimeMillis());
    }

    @Override
    public Collection<Void> onCallback(String key, long timestamp) {
      assertFalse(timerFired.get());

      timerFired.compareAndSet(false, true);

      return Collections.emptyList();
    }
  }
}
