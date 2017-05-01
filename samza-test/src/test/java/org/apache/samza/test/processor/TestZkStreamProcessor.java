package org.apache.samza.test.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifecycleListener;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.ClosableTask;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.StandaloneIntegrationTestHarness;
import org.apache.samza.test.StandaloneTestUtils;
import org.apache.samza.zk.TestZkUtils;
import org.junit.Assert;
import org.junit.Test;

//import static org.apache.samza.test.processor.TestStreamTask.endLatch;


public class TestZkStreamProcessor extends StandaloneIntegrationTestHarness {
  /**
   * Testing a basic identity stream task - reads data from a topic and writes it to another topic
   * (without any modifications)
   *
   * <p>
   * The standalone version in this test uses KafkaSystemFactory and it uses a SingleContainerGrouperFactory. Hence,
   * no matter how many tasks are present, it will always be run in a single processor instance. This simplifies testing
   */

  public static final String PROCESSOR_ID = "streamProcessor.id";

  public static StreamProcessorLifecycleListener listener = new StreamProcessorLifecycleListener() {
    @Override
    public void onStart() {
    }

    @Override
    public void onShutdown() {
    }

    @Override
    public void onFailure(Throwable t) {
    }
  };



  @Test
  public void testSingleStreamProcessor() {
    testStreamProcessor(new String [] {"1"});
  }

  @Test
  public void testTwoStreamProcessors() {
    testStreamProcessor(new String [] {"1", "2"});
  }

  @Test
  public void testFiveStreamProcessors() {
    testStreamProcessor(new String [] {"1", "2", "3", "4", "5"});
  }



  private void testStreamProcessor(String [] processorIds) {
    final String testSystem = "test-system";
    final String inputTopic = "numbers";
    final String outputTopic = "output";
    final int messageCount = 40;

    final Map<String, String> map = createConfigs(testSystem, inputTopic, outputTopic, messageCount);

    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);

    // create a latch of the size == number of messages
    TestStreamTask.endLatch = new CountDownLatch(messageCount);

    StreamProcessor [] streamProcessors = new StreamProcessor[processorIds.length];

    for(int i=0; i < processorIds.length; i++) {
      streamProcessors[i] = createStreamProcessor(processorIds[i], map);
    }
    produceMessages(0, inputTopic, messageCount);

    Thread [] threads = new Thread[processorIds.length];

    for(int i=0; i < processorIds.length; i++) {
      threads[i] = runInThread(streamProcessors[i], TestStreamTask.endLatch);
      threads[i].start();
    }

    try {
      for (Thread t : threads) {
        t.join(10000);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    verifyNumMessages(outputTopic, messageCount);
  }


  @Test
  public void testStreamProcessorWithAdd() {
    final String testSystem = "test-system";
    final String inputTopic = "numbers";
    final String outputTopic = "output";
    final int messageCount = 40;

    final Map<String, String> map = createConfigs(testSystem, inputTopic, outputTopic, messageCount);

    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);

    // create a latch of the size == number of messages
    TestStreamTask.endLatch = new CountDownLatch(2*messageCount);

    StreamProcessor sp = createStreamProcessor("1", map);

    produceMessages(0, inputTopic, messageCount);

    Thread t = runInThread(sp, TestStreamTask.endLatch);
    t.start();

    TestZkUtils.sleepMs(10000);


    //map.put("systems.test-system.samza.reset.offset", "true");
    //map.put("systems.test-system.samza.offset.default", "upcoming");
    StreamProcessor sp1 = createStreamProcessor("2", map);
    Thread t1 = runInThread(sp1, TestStreamTask.endLatch);
    t1.start();

    TestZkUtils.sleepMs(10000);

    // start messages from id messageCount and generate messageCount more
    produceMessages(messageCount, inputTopic, messageCount);

    try {
      t.join(10000);
      t1.join(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    verifyNumMessages(outputTopic, 2*messageCount);
  }

  private StreamProcessor createStreamProcessor(String pId, Map<String, String> map) {
    map.put(PROCESSOR_ID, pId);
    //Config configs = new MapConfig(map);

    StreamProcessor processor = new StreamProcessor(
        pId,
        new MapConfig(map),
        new HashMap<>(),
        TestStreamTask::new,
        listener);

    return processor;
  }

  private void createTopics(String inputTopic, String outputTopic) {
    TestUtils.createTopic(zkUtils(), inputTopic, 5, 1, servers(), new Properties());
    TestUtils.createTopic(zkUtils(), outputTopic, 5, 1, servers(), new Properties());
  }

  private Map<String, String> createConfigs( String testSystem, String inputTopic, String outputTopic, int messageCount) {
    Map<String, String> configs = new HashMap<>();
    configs.putAll(
        StandaloneTestUtils.getStandaloneConfigs("test-job",
            "org.apache.samza.test.processor.TestZkStreamProcessor.TestStreamTask"));
    configs.putAll(StandaloneTestUtils
        .getKafkaSystemConfigs(testSystem, bootstrapServers(), zkConnect(), null, StandaloneTestUtils.SerdeAlias.STRING,
            true));
    configs.put("task.inputs", String.format("%s.%s", testSystem, inputTopic));
    configs.put("app.messageCount", String.valueOf(messageCount));
    configs.put("app.outputTopic", outputTopic);
    configs.put("app.outputSystem", testSystem);
    configs.put(ZkConfig.ZK_CONNECT, zkConnect());

    configs.put("job.systemstreampartition.grouper.factory", "org.apache.samza.container.grouper.stream.GroupByPartitionFactory");
    configs.put("task.name.grouper.factory", "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory");


    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.zk.ZkJobCoordinatorFactory");

    return configs;
  }

  /**
   * Produces the provided number of messages to the topic.
   */
  private void produceMessages(final int start, String topic, int numMessages) {
    KafkaProducer producer = getKafkaProducer();
    for (int i = start; i < numMessages + start; i++) {
      try {
        System.out.println("producing " + i);
        producer.send(new ProducerRecord(topic, String.valueOf(i).getBytes())).get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Runs the provided stream processor by starting it, waiting on the provided latch with a timeout,
   * and then stopping it.
   */
  private Thread runInThread(final StreamProcessor processor, CountDownLatch latch) {
    Thread t = new Thread () {

      @Override
      public void run() {
        boolean latchResult = false;
        processor.start();
        try {
          //Thread.sleep(10000);
          latchResult = latch.await(100000, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          e.printStackTrace();
        } finally {
          if (!latchResult) {
            Assert.fail("StreamTask either failed to process all input or timed out!");
          }
        }
        processor.stop();
      }
    };
    return t;
  }

  /**
   * Consumes data from the topic until there are no new messages for a while
   * and asserts that the number of consumed messages is as expected.
   */
  private void verifyNumMessages(String topic, int expectedNumMessages) {
    KafkaConsumer consumer = getKafkaConsumer();
    consumer.subscribe(Collections.singletonList(topic));

    // since the message can come out of order - we will use a hash map
    Map<Integer, Boolean> map = new HashMap<>(expectedNumMessages);
    for (int i = 0; i < expectedNumMessages; i++) {
      map.put(i, false);
    }

    int count = 0;
    int emptyPollCount = 0;

    while (count < expectedNumMessages && emptyPollCount < 5) {
      ConsumerRecords records = consumer.poll(5000);
      if (!records.isEmpty()) {
        Iterator<ConsumerRecord> iterator = records.iterator();
        while (iterator.hasNext()) {
          ConsumerRecord record = iterator.next();
          //Assert.assertEquals(new String((byte[]) record.value()), String.valueOf(count));
          String val = new String((byte[]) record.value());
          System.out.println("Got value " + val);
          map.put(Integer.valueOf(val), true);
          count++;
        }
      } else {
        emptyPollCount++;
      }
    }
    // filter out numbers we did not get
    long numFalse = map.values().stream().filter(v->!v).count();
    Assert.assertEquals("didn't get this number of events ", 0, numFalse);
    Assert.assertEquals(expectedNumMessages, count);
  }


  // StreamTaskClass
  public static class TestStreamTask implements StreamTask, InitableTask {
    // static field since there's no other way to share state b/w a task instance and
    // stream processor when constructed from "task.class".
    static CountDownLatch endLatch;
    private int processedMessageCount = 0;
    private int expectedMessageCount;
    private String processorId;
    private String outputTopic;
    private String outputSystem;

    @Override
    public void init(Config config, TaskContext taskContext)
        throws Exception {
      //this.expectedMessageCount = config.getInt("app.messageCount");
      this.processorId = config.get(PROCESSOR_ID);
      this.outputTopic = config.get("app.outputTopic", "output");
      this.outputSystem = config.get("app.outputSystem", "test-system");
    }

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector,
        TaskCoordinator taskCoordinator)
        throws Exception {
      messageCollector.send(new OutgoingMessageEnvelope(new SystemStream(outputSystem, outputTopic),
              incomingMessageEnvelope.getMessage()));
      processedMessageCount++;
      String message = (String) incomingMessageEnvelope.getMessage();
      System.out.println("Stream processor " + processorId + ";offset=" + incomingMessageEnvelope.getOffset() + ";received " + message + "; ssp=" + incomingMessageEnvelope.getSystemStreamPartition());
      //if (processedMessageCount == expectedMessageCount) {
      synchronized (endLatch) {
        endLatch.countDown();
      }
      //}
    }
  }
}
