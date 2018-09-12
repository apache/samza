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
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.InMemorySystemConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.operators.KV;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
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
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.apache.samza.test.framework.system.CollectionStreamSystemSpec;
import org.junit.Assert;


/**
 * TestRunner provides apis to quickly set up tests for Samza low level and high level apis. Default running mode
 * for test is Single container without any distributed coordination service. Test runner maintains global job config
 * {@code configs} that are used to run the Samza job
 *
 * For single container mode following configs are set by default
 *  <ol>
 *    <li>"job.coordination.utils.factory" = {@link PassthroughCoordinationUtilsFactory}</li>
 *    <li>"job.coordination.factory" = {@link PassthroughJobCoordinatorFactory}</li>
 *    <li>"task.name.grouper.factory" = {@link SingleContainerGrouperFactory}</li>
 *    <li>"job.name" = "test-samza"</li>
 *    <li>"processor.id" = "1"</li>
 *    <li>"inmemory.scope = " Scope id generated to isolate the run for InMemorySystem</li>
 *  </ol>
 *
 */
public class TestRunner {

  private static final String JOB_NAME = "test-samza";
  public enum Mode {
    SINGLE_CONTAINER, MULTI_CONTAINER
  }

  private Map<String, String> configs;
  private Map<String, CollectionStreamSystemSpec> systems;
  private Class taskClass;
  private StreamApplication app;
  private String testId;
  private SystemFactory factory;

  /**
   * Mode defines single or multi container running configuration, by default a single container configuration is assumed
   */
  private Mode mode;

  private TestRunner() {
    this.testId = RandomStringUtils.random(10, true, true);
    this.systems = new HashMap<String, CollectionStreamSystemSpec>();
    this.configs = new HashMap<>();
    this.mode = Mode.SINGLE_CONTAINER;
    this.factory = new InMemorySystemFactory();
    configs.put(InMemorySystemConfig.INMEMORY_SCOPE, testId);
    configs.put(JobConfig.JOB_NAME(), JOB_NAME);
    configs.putIfAbsent(JobConfig.PROCESSOR_ID(), "1");
    configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
    configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    configs.putIfAbsent(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());
  }

  /**
   * Constructs a new {@link TestRunner} from following components
   * @param taskClass represent a class containing Samza job logic extending either {@link StreamTask} or {@link AsyncStreamTask}
   */
  private TestRunner(Class taskClass) {
    this();
    Preconditions.checkNotNull(taskClass);
    configs.put(TaskConfig.TASK_CLASS(), taskClass.getName());
    this.taskClass = taskClass;
  }

  /**
   * Constructs a new {@link TestRunner} from following components
   * @param app represent a class containing Samza job logic implementing {@link StreamApplication}
   */
  private TestRunner(StreamApplication app) {
    this();
    Preconditions.checkNotNull(app);
    this.app = app;
  }

  /**
   * Registers a system with TestRunner if not already registered and configures all the system configs to global
   * job configs
   */
  private void registerSystem(String systemName) {
    if (!systems.containsKey(systemName)) {
      systems.put(systemName, CollectionStreamSystemSpec.create(systemName, JOB_NAME));
      configs.putAll(systems.get(systemName).getSystemConfigs());
    }
  }

  /**
   * Creates an instance of {@link TestRunner} for Low Level Samza Api
   * @param taskClass represent a class extending either {@link StreamTask} or {@link AsyncStreamTask}
   * @return a {@link TestRunner} for {@code taskClass}
   */
  public static TestRunner of(Class taskClass) {
    Preconditions.checkNotNull(taskClass);
    Preconditions.checkState(
        StreamTask.class.isAssignableFrom(taskClass) || AsyncStreamTask.class.isAssignableFrom(taskClass));
    return new TestRunner(taskClass);
  }

  /**
   * Creates an instance of {@link TestRunner} for High Level/Fluent Samza Api
   * @param app represent a class representing Samza job by implementing {@link StreamApplication}
   * @return a {@link TestRunner} for {@code app}
   */
  public static TestRunner of(StreamApplication app) {
    Preconditions.checkNotNull(app);
    return new TestRunner(app);
  }

  /**
   * Only adds a config from {@code config} to global {@code configs} if they dont exist in it.
   * @param config represents the {@link Config} supposed to be added to global configs
   * @return calling instance of {@link TestRunner} with added configs if they don't exist
   */
  public TestRunner addConfigs(Config config) {
    Preconditions.checkNotNull(config);
    config.forEach(this.configs::putIfAbsent);
    return this;
  }

  /**
   * Adds a config to {@code configs} if its not already present. Overrides a config value for which key is already
   * exisiting in {@code configs}
   * @param key key of the config
   * @param value value of the config
   * @return calling instance of {@link TestRunner} with added config
   */
  public TestRunner addOverrideConfig(String key, String value) {
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(value);
    String configKeyPrefix = String.format(JobConfig.CONFIG_JOB_PREFIX(), JOB_NAME);
    configs.put(String.format("%s%s", configKeyPrefix, key), value);
    return this;
  }

  /**
   * Configures {@code stream} with the TestRunner, adds all the stream specific configs to global job configs.
   * <p>
   * Every stream belongs to a System (here a {@link CollectionStreamSystemSpec}), this utility also registers the system with
   * {@link TestRunner} if not registered already. Then it creates and initializes the stream partitions with messages for
   * the registered System
   * <p>
   * @param stream represents the stream that is supposed to be configured with {@link TestRunner}
   * @return calling instance of {@link TestRunner} with {@code stream} configured with it
   */
  public TestRunner addInputStream(CollectionStream stream) {
    Preconditions.checkNotNull(stream);
    registerSystem(stream.getSystemName());
    initializeInput(stream);
    stream.setTestId(testId);
    if (configs.containsKey(TaskConfig.INPUT_STREAMS())) {
      configs.put(TaskConfig.INPUT_STREAMS(),
          configs.get(TaskConfig.INPUT_STREAMS()).concat("," + stream.getSystemName() + "." + stream.getPhysicalName()));
    } else {
      configs.put(TaskConfig.INPUT_STREAMS(), stream.getSystemName() + "." + stream.getPhysicalName());
    }
    stream.getStreamConfig().forEach((key, val) -> {
        configs.putIfAbsent((String) key, (String) val);
      });

    return this;
  }

  /**
   * Creates an in memory stream with {@link InMemorySystemFactory} and initializes the metadata for the stream.
   * Initializes each partition of that stream with messages from {@code stream.getInitPartitions}
   *
   * @param stream represents the stream to initialize with the in memory system
   * @param <T> can represent a message or a KV {@link org.apache.samza.operators.KV}, key of which represents key of a
   *            {@link org.apache.samza.system.IncomingMessageEnvelope} or {@link org.apache.samza.system.OutgoingMessageEnvelope}
   *            and value represents the message
   */
  private <T> void initializeInput(CollectionStream stream) {
    Preconditions.checkNotNull(stream);
    Preconditions.checkState(stream.getInitPartitions().size() >= 1);
    String streamName = stream.getStreamName();
    String systemName = stream.getSystemName();
    Map<Integer, Iterable<T>> partitions = stream.getInitPartitions();
    StreamSpec spec = new StreamSpec(streamName, stream.getPhysicalName(), systemName, partitions.size());
    factory.getAdmin(systemName, new MapConfig(configs)).createStream(spec);
    SystemProducer producer = factory.getProducer(systemName, new MapConfig(configs), null);
    partitions.forEach((partitionId, partition) -> {
        partition.forEach(e -> {
            Object key = e instanceof KV ? ((KV) e).getKey() : null;
            Object value = e instanceof KV ? ((KV) e).getValue() : e;
            producer.send(systemName,
                new OutgoingMessageEnvelope(new SystemStream(systemName, stream.getPhysicalName()), Integer.valueOf(partitionId), key,
                    value));
          });
        producer.send(systemName,
            new OutgoingMessageEnvelope(new SystemStream(systemName, stream.getPhysicalName()), Integer.valueOf(partitionId), null,
                new EndOfStreamMessage(null)));
      });
  }

  /**
   * Configures {@code stream} with the TestRunner, adds all the stream specific configs to global job configs.
   * <p>
   * Every stream belongs to a System (here a {@link CollectionStreamSystemSpec}), this utility also registers the system with
   * {@link TestRunner} if not registered already. Then it creates the stream partitions with the registered System
   * <p>
   * @param stream represents the stream that is supposed to be configured with {@link TestRunner}
   * @return calling instance of {@link TestRunner} with {@code stream} configured with it
   */
  public TestRunner addOutputStream(CollectionStream stream) {
    Preconditions.checkNotNull(stream);
    Preconditions.checkState(stream.getInitPartitions().size() >= 1);
    registerSystem(stream.getSystemName());
    stream.setTestId(testId);
    StreamSpec spec = new StreamSpec(stream.getStreamName(), stream.getPhysicalName(), stream.getSystemName(), stream.getInitPartitions().size());
    factory
        .getAdmin(stream.getSystemName(), new MapConfig(configs))
        .createStream(spec);
    configs.putAll(stream.getStreamConfig());
    return this;
  }


  /**
   * Utility to run a test configured using TestRunner
   *
   * @param timeout time to wait for the high level application or low level task to finish. This timeout does not include
   *                input stream initialization time or the assertion time over output streams. This timeout just accounts
   *                for time that samza job takes run. Samza job won't be invoked with negative or zero timeout
   * @throws SamzaException if Samza job fails with exception and returns UnsuccessfulFinish as the statuscode
   */
  public void run(Duration timeout) {
    Preconditions.checkState((app == null && taskClass != null) || (app != null && taskClass == null),
        "TestRunner should run for Low Level Task api or High Level Application Api");
    Preconditions.checkState(!timeout.isZero() || !timeout.isNegative(), "Timeouts should be positive");
    SamzaApplication testApp = app == null ? (TaskApplication) appDesc -> appDesc.setTaskFactory(createTaskFactory()) : app;
    final LocalApplicationRunner runner = new LocalApplicationRunner(testApp, new MapConfig(configs));
    runner.run();
    boolean timedOut = !runner.waitForFinish(timeout);
    Assert.assertFalse("Timed out waiting for application to finish", timedOut);
    ApplicationStatus status = runner.status();
    if (status.getStatusCode() == ApplicationStatus.StatusCode.UnsuccessfulFinish) {
      throw new SamzaException(ExceptionUtils.getStackTrace(status.getThrowable()));
    }
  }
  /**
   * Utility to read the messages from a stream from the beginning, this is supposed to be used after executing the
   * TestRunner in order to assert over the streams (ex output streams).
   *
   * @param stream represents {@link CollectionStream} whose current state of partitions is requested to be fetched
   * @param timeout poll timeout in Ms
   * @param <T> represents type of message
   *
   * @return a map key of which represents the {@code partitionId} and value represents the current state of the partition
   *         i.e messages in the partition
   * @throws InterruptedException Thrown when a blocking poll has been interrupted by another thread.
   */
  public static <T> Map<Integer, List<T>> consumeStream(CollectionStream stream, Duration timeout) throws InterruptedException {
    Preconditions.checkNotNull(stream);
    Preconditions.checkNotNull(stream.getSystemName());
    String streamName = stream.getStreamName();
    String systemName = stream.getSystemName();
    Set<SystemStreamPartition> ssps = new HashSet<>();
    Set<String> streamNames = new HashSet<>();
    streamNames.add(streamName);
    SystemFactory factory = new InMemorySystemFactory();
    HashMap<String, String> config = new HashMap<>();
    config.put(InMemorySystemConfig.INMEMORY_SCOPE, stream.getTestId());
    Map<String, SystemStreamMetadata> metadata =
        factory.getAdmin(systemName, new MapConfig(config)).getSystemStreamMetadata(streamNames);
    SystemConsumer consumer = factory.getConsumer(systemName, new MapConfig(config), null);
    metadata.get(stream.getPhysicalName()).getSystemStreamPartitionMetadata().keySet().forEach(partition -> {
        SystemStreamPartition temp = new SystemStreamPartition(systemName, streamName, partition);
        ssps.add(temp);
        consumer.register(temp, "0");
      });

    long t = System.currentTimeMillis();
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> output = new HashMap<>();
    HashSet<SystemStreamPartition> didNotReachEndOfStream = new HashSet<>(ssps);
    while (System.currentTimeMillis() < t + timeout.toMillis()) {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> currentState = consumer.poll(ssps, 10);
      for (Map.Entry<SystemStreamPartition, List<IncomingMessageEnvelope>> entry : currentState.entrySet()) {
        SystemStreamPartition ssp = entry.getKey();
        output.computeIfAbsent(ssp, k -> new LinkedList<IncomingMessageEnvelope>());
        List<IncomingMessageEnvelope> currentBuffer = entry.getValue();
        Integer totalMessagesToFetch = Integer.valueOf(metadata.get(stream.getStreamName())
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
            entry -> entry.getValue().stream().map(e -> (T) e.getMessage()).collect(Collectors.toList())));
  }

  private TaskFactory createTaskFactory() {
    if (StreamTask.class.isAssignableFrom(taskClass)) {
      return (StreamTaskFactory) () -> {
        try {
          return (StreamTask) taskClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new SamzaException(String.format("Failed to instantiate StreamTask class %s", taskClass.getName()), e);
        }
      };
    } else if (AsyncStreamTask.class.isAssignableFrom(taskClass)) {
      return (AsyncStreamTaskFactory) () -> {
        try {
          return (AsyncStreamTask) taskClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          throw new SamzaException(String.format("Failed to instantiate AsyncStreamTask class %s", taskClass.getName()), e);
        }
      };
    }
    throw new SamzaException(String.format("Not supported task.class %s. task.class has to implement either StreamTask "
        + "or AsyncStreamTask", taskClass.getName()));
  }
}
