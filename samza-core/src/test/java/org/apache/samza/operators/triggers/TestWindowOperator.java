package org.apache.samza.operators.triggers;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import junit.framework.Assert;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.StreamGraphBuilder;
import org.apache.samza.operators.TestMessageEnvelope;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.windows.AccumulationMode;
import org.apache.samza.operators.windows.WindowKey;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestWindowOperator {
  private final MessageCollector messageCollector = mock(MessageCollector.class);
  private final TaskCoordinator taskCoordinator = mock(TaskCoordinator.class);
  private final List<Integer> mapOutput = new ArrayList<>();
  private final List<WindowPane> windowPanes = new ArrayList<>();
  private final List<Integer> integers = ImmutableList.of(1, 2, 1, 2, 1, 2, 1, 2, 3);
  private StreamOperatorTask task;

  @Before
  public void setup() throws Exception {
    mapOutput.clear();

    Config config = mock(Config.class);
    TaskContext taskContext = mock(TaskContext.class);
    when(taskContext.getSystemStreamPartitions()).thenReturn(ImmutableSet
        .of(new SystemStreamPartition("kafka", "integers", new Partition(0))));

    StreamGraphBuilder sgb = new TestStreamGraphBuilder();
    task = new StreamOperatorTask(sgb);
    task.init(config, taskContext);
  }

  @Test
  public void test() throws Exception {
    integers.forEach(n -> task.process(new TestEnvelope(n, n), messageCollector, taskCoordinator));
    Thread.sleep(1000);
    System.out.println("sleep done");
    task.window(messageCollector, taskCoordinator);
    Assert.assertEquals(windowPanes.size(), 5);
    Assert.assertEquals(windowPanes.get(0).getKey().getKey(), 1);
    Assert.assertEquals(((Collection)windowPanes.get(0).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(1).getKey().getKey(), 2);
    Assert.assertEquals(((Collection)windowPanes.get(1).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(2).getKey().getKey(), 1);
    Assert.assertEquals(((Collection)windowPanes.get(2).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(3).getKey().getKey(), 2);
    Assert.assertEquals(((Collection)windowPanes.get(3).getMessage()).size(), 2);

    Assert.assertEquals(windowPanes.get(4).getKey().getKey(), 3);
    Assert.assertEquals(((Collection)windowPanes.get(4).getMessage()).size(), 1);
  }


  private class TestStreamGraphBuilder implements StreamGraphBuilder {
    StreamSpec streamSpec = new StreamSpec("id1", "integers", "kafka");

    @Override
    public void init(StreamGraph graph, Config config) {
      MessageStream<MessageEnvelope<Integer, Integer>> inStream = graph.createInStream(streamSpec, null, null);
      Function<MessageEnvelope<Integer, Integer>, Integer> keyFn = m -> m.getKey();

      inStream
        .map(m -> {
          mapOutput.add(m.getKey());
          System.out.println("through map" + m);
          return m;
        })
        .window(Windows.keyedTumblingWindow(keyFn, Duration.ofSeconds(1)).setEarlyTrigger(Triggers.repeat(Triggers.count(2)))
          .setAccumulationMode(AccumulationMode.DISCARDING))
        .map(m -> {
          windowPanes.add(m);
          WindowKey<Integer> key = m.getKey();
          Collection<MessageEnvelope<Integer, Integer>> message = m.getMessage();
          ArrayList<MessageEnvelope<Integer, Integer>> list = new ArrayList<MessageEnvelope<Integer, Integer>>(message);
          System.out.println(list + "is window value");
          System.out.println(key + " is window key");
          return m;
        });
    }
  }

  private class TestEnvelope extends IncomingMessageEnvelope {
    TestEnvelope(int key, int msg) {
      super(new SystemStreamPartition("kafka", "integers", new Partition(0)), "1", key, msg);
    }
  }
}
