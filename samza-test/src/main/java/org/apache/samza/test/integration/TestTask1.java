package org.apache.samza.test.integration;

import org.apache.samza.operators.MessageStreams;
import org.apache.samza.operators.TriggerBuilder;
import org.apache.samza.operators.Windows;
import org.apache.samza.operators.data.IncomingSystemMessage;
import org.apache.samza.operators.data.Message;
import org.apache.samza.operators.internal.Operators;
import org.apache.samza.operators.task.StreamOperatorTask;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;
import java.util.Map;

/**
 * Created by jvenkatr on 10/23/16.
 */
public class TestTask1 implements StreamOperatorTask {


  public class PageView {
    final String region, userId, urlId;
    public PageView(IncomingSystemMessage message){
      Map<String, String> pageView = (Map<String,String>)message.getMessage();
      this.region = pageView.get("region");
      this.userId = pageView.get("userId");
      this.urlId = pageView.get("urlId");
    }

    @Override
    public String toString() {
      return "PageView{" +
          "region='" + region + '\'' +
          ", userId='" + userId + '\'' +
          ", urlId='" + urlId + '\'' +
          '}';
    }
  }

  public class PageViewMessage implements Message<String, PageView> {
    private final IncomingSystemMessage msg;
    private final PageView pageView;

    public PageViewMessage(IncomingSystemMessage msg) {
      this.msg = msg;
      this.pageView = new PageView(msg);
    }

    @Override
    public PageView getMessage() {
      return pageView;
    }

    @Override
    public String getKey() {
      return pageView.region;
    }

    @Override
    public long getTimestamp() {
      return System.currentTimeMillis();
    }
  }

  public void process1(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    System.out.println(envelope);
    Map<String, String> jsonMap = (Map<String, String>)envelope.getMessage();
  }

  public void initOperators(Collection<MessageStreams.SystemMessageStream> sources) {
    sources.forEach(source -> {
      /*
      source.map((msg -> new PageViewMessage(msg)))
            .window(Windows.intoSessionCounter(PageViewMessage::getKey)
                           .setTriggers(TriggerBuilder.earlyTriggerWhenExceedWndLen(2)))
            .sink((windowOutput, collector, coordinator)-> {
              String outputKey = windowOutput.getKey();
              Integer outputMsg = windowOutput.getMessage();
            });

      source.map((msg -> new PageViewMessage(msg)))
          .window(Windows.intoSessionCounter(PageViewMessage::getKey)
              .setTriggers(TriggerBuilder.earlyTriggerWhenExceedWndLen(2)))
          .sink((windowOutput, collector, coordinator)-> {
            String outputKey = windowOutput.getKey();
            Integer outputMsg = windowOutput.getMessage();
          });
       */

      source.map((msg -> new PageViewMessage(msg)))
            .window(Windows.intoSessions(PageViewMessage::getKey, PageViewMessage::getMessage)
                           .setTriggers(TriggerBuilder.<PageViewMessage, Collection<PageView>>earlyTriggerWhenExceedWndLen(20).addTimeoutSinceFirstMessage(10000)))
            .sink((windowOutput, collector, coordinator) -> {
              String outputKey = windowOutput.getKey();
              Collection<PageView> pageViews = windowOutput.getMessage();
              for (PageView view : pageViews) {
                System.out.println(view);
              }
              System.out.println("==========");
            })
           ;

    });


  }
}
