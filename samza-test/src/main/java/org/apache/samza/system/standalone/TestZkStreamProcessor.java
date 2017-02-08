package org.apache.samza.system.standalone;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaJobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.apache.samza.zk.ScheduleAfterDebounceTime;
import org.apache.samza.zk.ZkKeyBuilder;
import org.apache.samza.zk.ZkUtils;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;


public class TestZkStreamProcessor {
  private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(TestZkStreamProcessor.class);
  private static final String ZK_CONNECT_DEFAULT = "localhost:2182";
  private static final String KAFKA_CONNECT_DEFAULT = "localhost:9092";

  private static final ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10,
      new ThreadFactoryBuilder().setNameFormat("StreamProcessor-thread-%d").setDaemon(false).build());

  private Map<String, String> createConfigs(String testSystem, String inputTopic, String outputTopic, int messageCount) {
    Map<String, String> configs = new HashMap<>();
    configs.putAll(
        ZkTestUtils.getZkConfigs("test-job", "com.linkedin.samza.processor.IdentityStreamTask", ZK_CONNECT_DEFAULT));
    configs.putAll(ZkTestUtils.getKafkaSystemConfigs(testSystem, KAFKA_CONNECT_DEFAULT, ZK_CONNECT_DEFAULT, null,
        ZkTestUtils.SerdeAlias.STRING, true));
    configs.put("task.inputs", String.format("%s.%s", testSystem, inputTopic));
    configs.put("app.messageCount", String.valueOf(messageCount));
    configs.put("app.outputTopic", outputTopic);
    configs.put("app.outputSystem", testSystem);
    configs.put("systems."+testSystem + ".samza.factory","org.apache.samza.system.kafka.KafkaSystemFactory");
    configs.put("systems."+testSystem + ".producer.bootstrap.servers","ei3-kafka-kafka-aggregate-vip.stg.linkedin.com:10251");
    configs.put("systems."+testSystem + ".consumer.zookeeper.connect","zk-ei3-kafka.stg.linkedin.com:12913/kafka-cluster-aggregate");
    configs.put("job.systemstreampartition.grouper.factory","org.apache.samza.container.grouper.stream.GroupByPartitionFactory");
    configs.put("task.name.grouper.factory","org.apache.samza.container.grouper.task.SimpleGroupByContainerCountFactory");
    configs.put("task.class", "com.linkedin.samza.TestZkStreamProcessor$HelloWorldTask");
    configs.put("systems." + testSystem + ".samza.key.serde", "string");
    configs.put("systems." + testSystem + ".samza.msg.serde", "string");
    configs.put("serializers.registry.string.class", "org.apache.samza.serializers.StringSerdeFactory");
    //configs.put("serializers.registry.avro.class", "com.linkedin.samza.serializers.SchemaRegistrySerdeFactory");
    //configs.put("serializers.registry.avro.schemas","http://ei3-schema-registry-vip-1.stg.linkedin.com:10252/schemaRegistry/schemas");
    //configs.put("job.container.count", "3");
    configs.put("task.window.ms","10000");
    return configs;
  }

  public static void main(String[] args) {

    final String testSystem = "aggregate";
    final String inputTopic = "PageViewEvent";
    final String outputTopic = "output";
    final int messageCount = 20;


    TestZkStreamProcessor test = new TestZkStreamProcessor();

    final Config configs = new MapConfig(test.createConfigs(testSystem, inputTopic, outputTopic, messageCount));

    /// TEMP - clean up for testing
    JavaJobConfig jobConfig = new JavaJobConfig(configs);
    String groupName = String.format("%s-%s", jobConfig.getJobName(), jobConfig.getJobId());
    ZkConfig zkConfig = new ZkConfig(configs);
    ScheduleAfterDebounceTime debounceTimer = new ScheduleAfterDebounceTime();

    // start internal zookeeper
    Thread zkThread = startZookeeper();

    ZkConnection zkConnection = ZkUtils.createZkConnection(zkConfig.getZkConnect(), zkConfig.getZkSessionTimeoutMs());
    ZkClient zkClient = ZkUtils.createZkClient(zkConnection, zkConfig.getZkConnectionTimeoutMs());
    ZkUtils zkUtils = new ZkUtils(
        new ZkKeyBuilder(groupName),
        zkClient,
        zkConfig.getZkConnectionTimeoutMs()
        );

    zkUtils.deleteRoot();
    zkUtils.close();
    ///////////////////////////////

    int pid = 1;
    startAndStopProcessor(pid, configs,  0, 120000 );
    startAndStopProcessor(++pid, configs,  0, 120000 );
    startAndStopProcessor(++pid, configs,  0, 10000 );
    startAndStopProcessor(++pid, configs,  0, 120000 );
    startAndStopProcessor(++pid, configs,  11000, 120000 );
    startAndStopProcessor(++pid, configs,  12000, 120000 );

    /*
    int processorId = Integer.valueOf(args[0]);
    final StreamProcessor processor1 = new StreamProcessor(processorId, new MapConfig(configs), new HashMap<>());
    processor1.start();

    final StreamProcessor processor2 = new StreamProcessor(++processorId, new MapConfig(configs), new HashMap<>());
    processor2.start();

    final StreamProcessor processor3 = new StreamProcessor(++processorId, new MapConfig(configs), new HashMap<>());
    processor3.start();
    //try {
     // processor.awaitStart(10000);
    //} catch (InterruptedException e) {
     // e.printStackTrace();
    //}
    org.apache.kafka.common.utils.Utils.sleep(10000);

    processor2.stop();

    final StreamProcessor processor4 = new StreamProcessor(++processorId, new MapConfig(configs), new HashMap<>());
    processor4.start();

    org.apache.kafka.common.utils.Utils.sleep(120000);
    processor1.stop();
    //processor2.stop();
    processor3.stop();
    processor4.start();

    org.apache.kafka.common.utils.Utils.sleep(12000);
    processor4.stop();
*/
  }

  public static void startAndStopProcessor(final int pid, Config configs, long startDelayMs, long stopDelayMs) {
    final StreamProcessor processor = new StreamProcessor(pid, new MapConfig(configs), new HashMap<>());

    Runnable r = new Runnable () {
      @Override
      public void run() {
        LOG.info("About to start processor " + pid);
        processor.start();
        LOG.info("Started processor " + pid);
        org.apache.kafka.common.utils.Utils.sleep(stopDelayMs);
        LOG.info("Exiting processor " + pid);
        processor.stop();
      }
    };

    scheduledExecutorService.schedule(r, startDelayMs, TimeUnit.MILLISECONDS);
  }

  public static Thread startZookeeper() {
    Properties startupProperties = new Properties();
    startupProperties.put("tickTime", "2000");
    // The number of ticks that the initial
    //synchronization phase can take
    startupProperties.put("initLimit", "10");
    // The number of ticks that can pass between
    // sending a request and getting an acknowledgement
    startupProperties.put("syncLimit", "5");
    //the directory where the snapshot is stored.
    startupProperties.put("dataDir", "/tmp/zookeeper");
    //the port at which the clients will connect
    startupProperties.put("clientPort","2182");


    QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
    try {
      quorumConfiguration.parseProperties(startupProperties);
    } catch(Exception e) {
      throw new RuntimeException(e);
    }

    ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
    final ServerConfig configuration = new ServerConfig();
    configuration.readFrom(quorumConfiguration);

    Thread t = new Thread() {
      public void run() {
        try {
          zooKeeperServer.runFromConfig(configuration);
        } catch (IOException e) {
          LOG.error("ZooKeeper Failed", e);
        }
      }
    };
    t.setDaemon(true);
    t.start();

    return t;
  }

  public static class HelloWorldTask implements StreamTask, WindowableTask {
    private final static Logger LOG  = org.slf4j.LoggerFactory.getLogger("ZkHelloWorldTask");
    private final Map<SystemStreamPartition, Integer> eventsPerSSP = new HashMap<SystemStreamPartition, Integer>();

    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
      int currentVal = eventsPerSSP.getOrDefault(envelope.getSystemStreamPartition(), 0);
      eventsPerSSP.put(envelope.getSystemStreamPartition(), currentVal + 1);
      //collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "ProcessedArticleViewEvent"), envelope.getMessage()));
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator)
        throws Exception {

      eventsPerSSP.forEach((ssp, count) -> LOG.info("ssp=" + ssp + "; count=" + count));
    }
  }

}
