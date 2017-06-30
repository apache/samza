package org.apache.samza.example;

import org.apache.samza.application.StreamTaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KafkaSystem;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.CommandLine;

import java.util.Collections;

/**
 * Created by yipan on 6/21/17.
 */
public class TaskApplicationExample {

  static class MyStreamTaskFactory implements StreamTaskFactory {

    @Override
    public StreamTask createInstance() {
      return new StreamTask() {
        @Override
        public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
          return;
        }
      };
    }
  }

  // local execution mode
  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));

    StreamTaskApplication app = StreamTaskApplication.create(config, new MyStreamTaskFactory());

    KafkaSystem kafkaSystem = KafkaSystem.create("kafka")
        .withBootstrapServers("localhost:9192")
        .withConsumerProperties(config)
        .withProducerProperties(config);

    StreamDescriptor<String, PageViewEvent> input = StreamDescriptor.<String, PageViewEvent>create("myPageViewEvent")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);
    StreamDescriptor<String, PageViewCount> output = StreamDescriptor.<String, PageViewCount>create("pageViewEventPerMemberStream")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);

    app.addInputs(Collections.singletonList(input)).addOutputs(Collections.singletonList(output)).run();
    app.waitForFinish();
  }

  class PageViewEvent {
    String pageId;
    String memberId;
    long timestamp;

    PageViewEvent(String pageId, String memberId, long timestamp) {
      this.pageId = pageId;
      this.memberId = memberId;
      this.timestamp = timestamp;
    }
  }

  static class PageViewCount {
    String memberId;
    long timestamp;
    int count;

    PageViewCount(WindowPane<String, Integer> m) {
      this.memberId = m.getKey().getKey();
      this.timestamp = Long.valueOf(m.getKey().getPaneId());
      this.count = m.getMessage();
    }
  }

}
