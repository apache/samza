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
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.KafkaInputDescriptor;
import org.apache.samza.system.kafka.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;


/**
 * Test class to create an {@link StreamApplication} instance
 */
public class TestStreamApplication implements StreamApplication {

  private final String systemName;
  private final String inputTopic;
  private final String outputTopic;
  private final String appName;
  private final String processorName;

  private TestStreamApplication(String systemName, String inputTopic, String outputTopic,
      String appName, String processorName) {
    this.systemName = systemName;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
    this.appName = appName;
    this.processorName = processorName;
  }

  @Override
  public void describe(StreamApplicationDescriptor streamAppDesc) {
    KafkaSystemDescriptor ksd = new KafkaSystemDescriptor(systemName);
    KafkaInputDescriptor<String> isd = ksd.getInputDescriptor(inputTopic, new NoOpSerde<>());
    KafkaOutputDescriptor<String> osd = ksd.getOutputDescriptor(outputTopic, new StringSerde());
    MessageStream<String> inputStream = streamAppDesc.getInputStream(isd);
    OutputStream<String> outputStream = streamAppDesc.getOutputStream(osd);
    inputStream.map(new TestMapFunction(appName, processorName)).sendTo(outputStream);
  }

  public interface StreamApplicationCallback {
    void onMessage(TestKafkaEvent m);
  }

  public static class TestMapFunction implements MapFunction<String, String> {
    private final String appName;
    private final String processorName;

    private transient CountDownLatch latch1;
    private transient CountDownLatch latch2;
    private transient StreamApplicationCallback callback;

    TestMapFunction(String appName, String processorName) {
      this.appName = appName;
      this.processorName = processorName;
    }

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
      String systemName,
      String inputTopic,
      String outputTopic,
      CountDownLatch processedMessageLatch,
      StreamApplicationCallback callback,
      CountDownLatch kafkaEventsConsumedLatch,
      Config config) {
    String appName = new ApplicationConfig(config).getGlobalAppId();
    String processorName = config.get(JobConfig.PROCESSOR_ID());
    registerLatches(processedMessageLatch, kafkaEventsConsumedLatch, callback, appName, processorName);

    StreamApplication app = new TestStreamApplication(systemName, inputTopic, outputTopic, appName, processorName);
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
