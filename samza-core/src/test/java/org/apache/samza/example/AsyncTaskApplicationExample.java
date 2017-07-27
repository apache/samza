package org.apache.samza.example;

import java.util.Collections;
import org.apache.samza.application.AsyncStreamTaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KafkaSystem;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.util.CommandLine;


/**
 * Created by yipan on 7/24/17.
 */
public class AsyncTaskApplicationExample {
  static class MyStreamTaskFactory implements AsyncStreamTaskFactory {

    @Override
    public AsyncStreamTask createInstance() {
      return (envelope, collector, coordinator, callback) -> {
        callback.complete();
        return;
      };
    }
  }

  // local execution mode
  public static void main(String[] args) {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));

    AsyncStreamTaskApplication app = AsyncStreamTaskApplication.create(config, new MyStreamTaskFactory());

    KafkaSystem kafkaSystem = KafkaSystem.create("kafka")
        .withBootstrapServers("localhost:9192")
        .withConsumerProperties(config)
        .withProducerProperties(config);

    StreamDescriptor.Input<String, PageViewEvent> input = StreamDescriptor.<String, PageViewEvent>input("myPageViewEvent")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);
    StreamDescriptor.Output<String, PageViewCount> output = StreamDescriptor.<String, PageViewCount>output("pageViewEventPerMemberStream")
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

    PageViewCount(String memberId, long timestamp, int count) {
      this.memberId = memberId;
      this.timestamp = timestamp;
      this.count = count;
    }
  }

}
