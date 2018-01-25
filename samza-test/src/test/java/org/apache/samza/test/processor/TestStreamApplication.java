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
import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;


/**
 * Test class to create an {@link StreamApplication} instance
 */
public class TestStreamApplication implements StreamApplication, Serializable {

  private final String inputTopic;
  private final String outputTopic;
  private final String appName;
  private final String processorName;

  private TestStreamApplication(String inputTopic, String outputTopic, String appName, String processorName) {
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
    this.appName = appName;
    this.processorName = processorName;
  }

  @Override
  public void init(StreamGraph graph, Config config) {
    MessageStream<String> inputStream = graph.getInputStream(inputTopic, new NoOpSerde<String>());
    OutputStream<String> outputStream = graph.getOutputStream(outputTopic, new StringSerde());
    inputStream.map(new MapFunction<String, String>() {
      transient CountDownLatch latch1;
      transient CountDownLatch latch2;
      transient StreamApplicationCallback callback;

      @Override
      public String apply(String message) {
        TestKafkaEvent incomingMessage = TestKafkaEvent.fromString(message);
        if (callback != null) {
          callback.onMessage(incomingMessage);
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
        SharedContextFactories.SharedContextFactory contextFactory =
            SharedContextFactories.getGlobalSharedContextFactory(appName).getProcessorSharedContextFactory(processorName);
        this.latch1 = (CountDownLatch) contextFactory.getSharedObject("processedMsgLatch");
        this.latch2 = (CountDownLatch) contextFactory.getSharedObject("kafkaMsgsConsumedLatch");
        this.callback = (StreamApplicationCallback) contextFactory.getSharedObject("callback");
      }
    }).sendTo(outputStream);
  }

  public interface StreamApplicationCallback {
    void onMessage(TestKafkaEvent m);
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
    String appName = String.format("%s-%s", config.get(ApplicationConfig.APP_NAME), config.get(ApplicationConfig.APP_ID));
    String processorName = config.get(JobConfig.PROCESSOR_ID());
    registerLatches(processedMessageLatch, kafkaEventsConsumedLatch, callback, appName, processorName);

    StreamApplication app = new TestStreamApplication(inputTopic, outputTopic, appName, processorName);
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
