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

package org.apache.samza.test.processor;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplications;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.task.TaskContext;

import static com.google.common.base.Preconditions.*;


/**
 * Created by yipan on 12/11/17.
 */
public class TestStreamApplication implements Serializable {

  static AtomicBoolean hasSecondProcessorJoined = new AtomicBoolean(false);

  static CountDownLatch secondProcessorRegistered = new CountDownLatch(1);

  static CountDownLatch processedLatch = new CountDownLatch(1);

  public interface StreamApplicationCallback {
    void onMessage();
  }

  public static class TestKafkaEvent implements Serializable {

    // Actual content of the event.
    private String eventData;

    // Contains Integer value, which is greater than previous message id.
    private String eventId;

    TestKafkaEvent(String eventId, String eventData) {
      this.eventData = eventData;
      this.eventId = eventId;
    }

    String getEventId() {
      return eventId;
    }

    String getEventData() {
      return eventData;
    }

    @Override
    public String toString() {
      return eventId + "|" + eventData;
    }

    static TestKafkaEvent fromString(String message) {
      String[] messageComponents = message.split("|");
      return new TestKafkaEvent(messageComponents[0], messageComponents[1]);
    }
  }

  public static StreamApplication getInstance(
      String inputTopic,
      String outputTopic,
      CountDownLatch processedMessageLatch,
      StreamApplicationCallback callback,
      CountDownLatch kafkaEventsConsumedLatch,
      Config config) throws IOException {
    StreamApplication app = StreamApplications.createStreamApp(config);
    String appName = app.getGlobalAppId();
    String processorName = config.get(JobConfig.PROCESSOR_ID());
    registerLatches(processedMessageLatch, kafkaEventsConsumedLatch, callback, appName, processorName);
    MessageStream<String> inputStream = app.openInput(inputTopic, new NoOpSerde<String>());
    OutputStream<String> outputStream = app.openOutput(outputTopic, new StringSerde());
    inputStream
        .map(new MapFunction<String, String>() {
          transient CountDownLatch latch1;
          transient CountDownLatch latch2;
          transient StreamApplicationCallback callback;

          @Override
          public String apply(String message) {
            TestKafkaEvent incomingMessage = TestKafkaEvent.fromString(message);
            if (callback != null) {
              callback.onMessage();
            }
            if (latch1 != null) {
              latch1.countDown();
            }
            if (latch2 != null) {
              latch2.countDown();
            }
            return incomingMessage.toString();
          }

          private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            SharedContextFactories.SharedContextFactory contextFactory = SharedContextFactories.getGlobalSharedContextFactory(appName).getProcessorSharedContextFactory(processorName);
            this.latch1 = (CountDownLatch) contextFactory.getSharedObject("processedMsgLatch");
            this.latch2 = (CountDownLatch) contextFactory.getSharedObject("kafkaMsgsConsumedLatch");
            this.callback = (StreamApplicationCallback) contextFactory.getSharedObject("callback");
          }
        })
        .sendTo(outputStream);
    return app;
  }

  private static void registerLatches(CountDownLatch processedMessageLatch, CountDownLatch kafkaEventsConsumedLatch,
      StreamApplicationCallback callback, String appName, String processorName) {
    SharedContextFactories.SharedContextFactory contextFactory = SharedContextFactories.getGlobalSharedContextFactory(appName).getProcessorSharedContextFactory(processorName);
    contextFactory.addSharedObject("processedMsgLatch", processedMessageLatch);
    contextFactory.addSharedObject("kafkaMsgsConsumedLatch", kafkaEventsConsumedLatch);
    contextFactory.addSharedObject("callback", callback);
  }
}
