package org.apache.samza.test.integration;

import org.apache.samza.operators.MessageStreams;
import org.apache.samza.operators.Windows;
import org.apache.samza.operators.internal.Operators;
import org.apache.samza.operators.task.StreamOperatorTask;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.Collection;

/**
 * Created by jvenkatr on 10/23/16.
 */
public class TestTask1 implements StreamOperatorTask {

  public void process1(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {
    System.out.println(envelope);
  }

  @Override
  public void initOperators(Collection<MessageStreams.SystemMessageStream> sources) {
    sources.forEach(source -> {
       //source.window(Windows.intoSessionCounter())
    });
  }
}
