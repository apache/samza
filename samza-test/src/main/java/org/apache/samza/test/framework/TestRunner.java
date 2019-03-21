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

package org.apache.samza.test.framework;

import com.google.common.base.Preconditions;
import java.io.File;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.application.LegacyTaskApplication;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.InMemorySystemConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.execution.JobPlanner;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metadatastore.InMemoryMetadataStoreFactory;
import org.apache.samza.operators.KV;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.descriptors.StreamDescriptor;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.StreamTask;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * TestRunner provides APIs to set up integration tests for a Samza application.
 * Running mode for test is Single container mode
 * Test sets following configuration for the application
 *
 * The following configs are set by default
 *  <ol>
 *    <li>"job.coordination.factory" = {@link PassthroughJobCoordinatorFactory}</li>
 *    <li>"task.name.grouper.factory" = {@link SingleContainerGrouperFactory}</li>
 *    <li>"app.name" = "test-samza"</li>
 *    <li>"processor.id" = "1"</li>
 *    <li>"job.default.system" = {@code JOB_DEFAULT_SYSTEM}</li>
 *    <li>"job.host-affinity.enabled" = "false"</li>
 *  </ol>
 *
 * TestRunner only supports NoOpSerde i.e. inputs to Test Framework should be deserialized
 */
public class TestRunner {
  private static final Logger LOG = LoggerFactory.getLogger(TestRunner.class);
  private static final String JOB_DEFAULT_SYSTEM = "default-samza-system";
  private static final String APP_NAME = "samza-test";

  private Map<String, String> configs;
  private SamzaApplication app;
  private ExternalContext externalContext;
  /*
   * inMemoryScope is a unique global key per TestRunner, this key when configured with {@link InMemorySystemDescriptor}
   * provides an isolated state to run with in memory system
   */
  private String inMemoryScope;

  private TestRunner() {
    this.configs = new HashMap<>();
    this.inMemoryScope = RandomStringUtils.random(10, true, true);
    configs.put(ApplicationConfig.APP_NAME, APP_NAME);
    configs.put(JobConfig.PROCESSOR_ID(), "1");
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    configs.put(JobConfig.STARTPOINT_METADATA_STORE_FACTORY(), InMemoryMetadataStoreFactory.class.getCanonicalName());
    configs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());
    // Changing the base directory for non-changelog stores used by Samza application to separate the
    // on-disk store locations for concurrently executing tests
    configs.put(JobConfig.JOB_NON_LOGGED_STORE_BASE_DIR(),
        new File(System.getProperty("java.io.tmpdir"), this.inMemoryScope + "-non-logged").getAbsolutePath());
    configs.put(JobConfig.JOB_LOGGED_STORE_BASE_DIR(),
        new File(System.getProperty("java.io.tmpdir"), this.inMemoryScope + "-logged").getAbsolutePath());
    addConfig(JobConfig.JOB_DEFAULT_SYSTEM(), JOB_DEFAULT_SYSTEM);
    // Disabling host affinity since it requires reading locality information from a Kafka coordinator stream
    addConfig(ClusterManagerConfig.JOB_HOST_AFFINITY_ENABLED, Boolean.FALSE.toString());
    addConfig(InMemorySystemConfig.INMEMORY_SCOPE, inMemoryScope);
    addConfig(new InMemorySystemDescriptor(JOB_DEFAULT_SYSTEM).withInMemoryScope(inMemoryScope).toConfig());
  }

  /**
   * Constructs a new {@link TestRunner} from following components
   * @param taskClass containing Samza job logic extending either {@link StreamTask} or {@link AsyncStreamTask}
   */
  private TestRunner(Class taskClass) {
    this();
    Preconditions.checkNotNull(taskClass);
    configs.put(TaskConfig.TASK_CLASS(), taskClass.getName());
    this.app = new LegacyTaskApplication(taskClass.getName());
  }

  /**
   * Constructs a new {@link TestRunner} from following components
   * @param app a {@link SamzaApplication}
   */
  private TestRunner(SamzaApplication app) {
    this();
    Preconditions.checkNotNull(app);
    this.app = app;
  }

  /**
   * Creates an instance of {@link TestRunner} for Low Level Samza Api
   * @param taskClass samza job extending either {@link StreamTask} or {@link AsyncStreamTask}
   * @return this {@link TestRunner}
   */
  public static TestRunner of(Class taskClass) {
    Preconditions.checkNotNull(taskClass);
    Preconditions.checkState(
        StreamTask.class.isAssignableFrom(taskClass) || AsyncStreamTask.class.isAssignableFrom(taskClass));
    return new TestRunner(taskClass);
  }

  /**
   * Creates an instance of {@link TestRunner} for a {@link SamzaApplication}
   * @param app a {@link SamzaApplication}
   * @return this {@link TestRunner}
   */
  public static TestRunner of(SamzaApplication app) {
    Preconditions.checkNotNull(app);
    return new TestRunner(app);
  }

  /**
   * Adds a config to Samza application. This config takes precedence over default configs and descriptor generated configs
   *
   * @param key of the config
   * @param value of the config
   * @return this {@link TestRunner}
   */
  public TestRunner addConfig(String key, String value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    configs.put(key, value);
    return this;
  }

  /**
   * Adds a config to Samza application. This config takes precedence over default configs and descriptor generated configs
   * @param config configs for the application
   * @return this {@link TestRunner}
   */
  public TestRunner addConfig(Map<String, String> config) {
    Preconditions.checkNotNull(config);
    configs.putAll(config);
    return this;
  }

  /**
   * Passes the user provided external context to {@link LocalApplicationRunner}
   *
   * @param externalContext external context provided by user
   * @return this {@link TestRunner}
   */
  public TestRunner addExternalContext(ExternalContext externalContext) {
    Preconditions.checkNotNull(externalContext);
    this.externalContext = externalContext;
    return this;
  }

  /**
   * Adds the provided input stream with mock data to the test application.
   *
   * @param descriptor describes the stream that is supposed to be input to Samza application
   * @param messages messages used to initialize the single partition stream. These message should always be deserialized
   * @param <StreamMessageType> a message with null key or a KV {@link org.apache.samza.operators.KV}.
   *                            key of KV represents key of {@link org.apache.samza.system.IncomingMessageEnvelope} or
   *                           {@link org.apache.samza.system.OutgoingMessageEnvelope} and value is message
   * @return this {@link TestRunner}
   */
  public <StreamMessageType> TestRunner addInputStream(InMemoryInputDescriptor descriptor,
      List<StreamMessageType> messages) {
    Preconditions.checkNotNull(descriptor, messages);
    Map<Integer, Iterable<StreamMessageType>> partitionData = new HashMap<Integer, Iterable<StreamMessageType>>();
    partitionData.put(0, messages);
    initializeInMemoryInputStream(descriptor, partitionData);
    return this;
  }

  /**
   * Adds the provided input stream with mock data to the test application. Default configs and user added configs have
   * a higher precedence over system and stream descriptor generated configs.
   * @param descriptor describes the stream that is supposed to be input to Samza application
   * @param messages map whose key is partitionId and value is messages in the partition. These message should always
   *                 be deserialized
   * @param <StreamMessageType> message with null key or a KV {@link org.apache.samza.operators.KV}.
   *                           A key of which represents key of {@link org.apache.samza.system.IncomingMessageEnvelope} or
   *                           {@link org.apache.samza.system.OutgoingMessageEnvelope} and value is message
   * @return this {@link TestRunner}
   */
  public <StreamMessageType> TestRunner addInputStream(InMemoryInputDescriptor descriptor,
      Map<Integer, ? extends Iterable<StreamMessageType>> messages) {
    Preconditions.checkNotNull(descriptor, messages);
    Map<Integer, Iterable<StreamMessageType>> partitionData = new HashMap<Integer, Iterable<StreamMessageType>>();
    partitionData.putAll(messages);
    initializeInMemoryInputStream(descriptor, partitionData);
    return this;
  }

  /**
   * Adds the provided output stream to the test application. Default configs and user added configs have a higher
   * precedence over system and stream descriptor generated configs.
   * @param streamDescriptor describes the stream that is supposed to be output for the Samza application
   * @param partitionCount partition count of output stream
   * @return this {@link TestRunner}
   */
  public TestRunner addOutputStream(InMemoryOutputDescriptor<?> streamDescriptor, int partitionCount) {
    Preconditions.checkNotNull(streamDescriptor);
    Preconditions.checkState(partitionCount >= 1);
    InMemorySystemDescriptor imsd = (InMemorySystemDescriptor) streamDescriptor.getSystemDescriptor();
    imsd.withInMemoryScope(this.inMemoryScope);
    Config config = new MapConfig(streamDescriptor.toConfig(), streamDescriptor.getSystemDescriptor().toConfig());
    InMemorySystemFactory factory = new InMemorySystemFactory();
    String physicalName = (String) streamDescriptor.getPhysicalName().orElse(streamDescriptor.getStreamId());
    StreamSpec spec = new StreamSpec(streamDescriptor.getStreamId(), physicalName, streamDescriptor.getSystemName(),
        partitionCount);
    factory
        .getAdmin(streamDescriptor.getSystemName(), config)
        .createStream(spec);
    addConfig(streamDescriptor.toConfig());
    addConfig(streamDescriptor.getSystemDescriptor().toConfig());
    addSerdeConfigs(streamDescriptor);
    return this;
  }

  /**
   * Run the application with the specified timeout
   *
   * @param timeout time to wait for the application to finish. This timeout does not include
   *                input stream initialization time or the assertion time over output streams. This timeout just accounts
   *                for time that samza job takes run. Timeout must be greater than 0.
   * @throws SamzaException if Samza job fails with exception and returns UnsuccessfulFinish as the statuscode
   */
  public void run(Duration timeout) {
    Preconditions.checkNotNull(app);
    Preconditions.checkState(!timeout.isZero() || !timeout.isNegative(), "Timeouts should be positive");
    // Cleaning store directories to ensure current run does not pick up state from previous run
    deleteStoreDirectories();
    Config config = new MapConfig(JobPlanner.generateSingleJobConfig(configs));
    final LocalApplicationRunner runner = new LocalApplicationRunner(app, config);
    runner.run(externalContext);
    if (!runner.waitForFinish(timeout)) {
      throw new SamzaException("Timed out waiting for application to finish");
    }
    ApplicationStatus status = runner.status();
    deleteStoreDirectories();
    if (status.getStatusCode() == ApplicationStatus.StatusCode.UnsuccessfulFinish) {
      throw new SamzaException("Application could not finish successfully", status.getThrowable());
    }
  }

  /**
   * Gets the contents of the output stream represented by {@code outputDescriptor} after {@link TestRunner#run(Duration)}
   * has completed
   *
   * @param outputDescriptor describes the stream to be consumed
   * @param timeout timeout for consumption of stream in Ms
   * @param <StreamMessageType> type of message
   *
   * @return a map whose key is {@code partitionId} and value is messages in partition
   * @throws SamzaException Thrown when a poll is incomplete
   */
  public static <StreamMessageType> Map<Integer, List<StreamMessageType>> consumeStream(
      InMemoryOutputDescriptor outputDescriptor, Duration timeout) throws SamzaException {
    Preconditions.checkNotNull(outputDescriptor);
    String streamId = outputDescriptor.getStreamId();
    String systemName = outputDescriptor.getSystemName();
    Set<SystemStreamPartition> ssps = new HashSet<>();
    Set<String> streamIds = new HashSet<>();
    streamIds.add(streamId);
    SystemFactory factory = new InMemorySystemFactory();
    Config config = new MapConfig(outputDescriptor.toConfig(), outputDescriptor.getSystemDescriptor().toConfig());
    Map<String, SystemStreamMetadata> metadata = factory.getAdmin(systemName, config).getSystemStreamMetadata(streamIds);
    SystemConsumer consumer = factory.getConsumer(systemName, config, null);
    String name = (String) outputDescriptor.getPhysicalName().orElse(streamId);
    metadata.get(name).getSystemStreamPartitionMetadata().keySet().forEach(partition -> {
        SystemStreamPartition temp = new SystemStreamPartition(systemName, streamId, partition);
        ssps.add(temp);
        consumer.register(temp, "0");
      });

    long t = System.currentTimeMillis();
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> output = new HashMap<>();
    HashSet<SystemStreamPartition> didNotReachEndOfStream = new HashSet<>(ssps);
    while (System.currentTimeMillis() < t + timeout.toMillis()) {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> currentState = null;
      try {
        currentState = consumer.poll(ssps, 10);
      } catch (InterruptedException e) {
        throw new SamzaException("Timed out while consuming stream \n" + e.getMessage());
      }
      for (Map.Entry<SystemStreamPartition, List<IncomingMessageEnvelope>> entry : currentState.entrySet()) {
        SystemStreamPartition ssp = entry.getKey();
        output.computeIfAbsent(ssp, k -> new LinkedList<IncomingMessageEnvelope>());
        List<IncomingMessageEnvelope> currentBuffer = entry.getValue();
        Integer totalMessagesToFetch = Integer.valueOf(metadata.get(outputDescriptor.getStreamId())
            .getSystemStreamPartitionMetadata()
            .get(ssp.getPartition())
            .getNewestOffset());
        if (output.get(ssp).size() + currentBuffer.size() == totalMessagesToFetch) {
          didNotReachEndOfStream.remove(entry.getKey());
          ssps.remove(entry.getKey());
        }
        output.get(ssp).addAll(currentBuffer);
      }
      if (didNotReachEndOfStream.isEmpty()) {
        break;
      }
    }

    if (!didNotReachEndOfStream.isEmpty()) {
      throw new IllegalStateException("Could not poll for all system stream partitions");
    }

    return output.entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> entry.getKey().getPartition().getPartitionId(),
            entry -> entry.getValue().stream().map(e -> (StreamMessageType) e.getMessage()).collect(Collectors.toList())));
  }

  /**
   * Creates an in memory stream with {@link InMemorySystemFactory} and feeds its partition with stream of messages
   * @param partitionData key of the map represents partitionId and value represents messages in the partition
   * @param descriptor describes a stream to initialize with the in memory system
   */
  private <StreamMessageType> void initializeInMemoryInputStream(InMemoryInputDescriptor<?> descriptor,
      Map<Integer, Iterable<StreamMessageType>> partitionData) {
    String systemName = descriptor.getSystemName();
    String streamName = (String) descriptor.getPhysicalName().orElse(descriptor.getStreamId());
    if (configs.containsKey(TaskConfig.INPUT_STREAMS())) {
      configs.put(TaskConfig.INPUT_STREAMS(),
          configs.get(TaskConfig.INPUT_STREAMS()).concat("," + systemName + "." + streamName));
    } else {
      configs.put(TaskConfig.INPUT_STREAMS(), systemName + "." + streamName);
    }
    InMemorySystemDescriptor imsd = (InMemorySystemDescriptor) descriptor.getSystemDescriptor();
    imsd.withInMemoryScope(this.inMemoryScope);
    addConfig(descriptor.toConfig());
    addConfig(descriptor.getSystemDescriptor().toConfig());
    addSerdeConfigs(descriptor);
    StreamSpec spec = new StreamSpec(descriptor.getStreamId(), streamName, systemName, partitionData.size());
    SystemFactory factory = new InMemorySystemFactory();
    Config config = new MapConfig(descriptor.toConfig(), descriptor.getSystemDescriptor().toConfig());
    factory.getAdmin(systemName, config).createStream(spec);
    SystemProducer producer = factory.getProducer(systemName, config, null);
    SystemStream sysStream = new SystemStream(systemName, streamName);
    partitionData.forEach((partitionId, partition) -> {
        partition.forEach(e -> {
            Object key = e instanceof KV ? ((KV) e).getKey() : null;
            Object value = e instanceof KV ? ((KV) e).getValue() : e;
            producer.send(systemName, new OutgoingMessageEnvelope(sysStream, Integer.valueOf(partitionId), key, value));
          });
        producer.send(systemName, new OutgoingMessageEnvelope(sysStream, Integer.valueOf(partitionId), null,
            new EndOfStreamMessage(null)));
      });
  }

  private void deleteStoreDirectories() {
    Preconditions.checkNotNull(configs.get(JobConfig.JOB_LOGGED_STORE_BASE_DIR()));
    Preconditions.checkNotNull(configs.get(JobConfig.JOB_NON_LOGGED_STORE_BASE_DIR()));
    deleteDirectory(configs.get(JobConfig.JOB_NON_LOGGED_STORE_BASE_DIR()));
    deleteDirectory(configs.get(JobConfig.JOB_LOGGED_STORE_BASE_DIR()));
  }

  private void deleteDirectory(String path) {
    File dir = new File(path);
    LOG.info("Deleting the directory " + path);
    FileUtil.rm(dir);
    if (dir.exists()) {
      LOG.warn("Could not delete the directory " + path);
    }
  }

  /**
   * Test Framework only supports NoOpSerde. This method ensures null key and msg serde config for input and output streams
   * takes preference when configs are merged in {@link org.apache.samza.execution.JobPlanner#getExecutionPlan}
   * over {@link org.apache.samza.application.descriptors.ApplicationDescriptor} generated configs
   */
  private void addSerdeConfigs(StreamDescriptor descriptor) {
    String streamIdPrefix = String.format(StreamConfig.STREAM_ID_PREFIX(), descriptor.getStreamId());
    String keySerdeConfigKey = streamIdPrefix + StreamConfig.KEY_SERDE();
    String msgSerdeConfigKey = streamIdPrefix + StreamConfig.MSG_SERDE();
    this.configs.put(keySerdeConfigKey, null);
    this.configs.put(msgSerdeConfigKey, null);
  }
}
