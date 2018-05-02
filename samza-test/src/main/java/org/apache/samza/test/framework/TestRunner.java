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
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.StreamTask;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.apache.samza.test.framework.system.CollectionStreamSystem;


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
 *  </ol>
 *
 */
public class TestRunner {

  private static final String JOB_NAME = "test-samza";

  public enum Mode {
    SINGLE_CONTAINER, MULTI_CONTAINER
  }

  private static Map<String, String> configs;
  private static Map<String, Object> systems;
  private Class taskClass;
  private StreamApplication app;

  /**
   * Mode defines single or multi container running configuration, by default a single container configuration is assumed
   */
  private Mode mode;

  private TestRunner() {
    this.configs = new HashMap<>();
    this.systems = new HashMap<>();
    this.mode = Mode.SINGLE_CONTAINER;
    configs.put(JobConfig.JOB_NAME(), JOB_NAME);
    configs.putIfAbsent(JobConfig.PROCESSOR_ID(), "1");
    configs.putIfAbsent(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY,
        PassthroughCoordinationUtilsFactory.class.getName());
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
  private void registerSystem(CollectionStreamSystem system) {
    if (!systems.containsKey(system.getSystemName())) {
      systems.put(system.getSystemName(), system);
      configs.putAll(system.getSystemConfigs());
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
   * Only adds the key, value from {@code config} to {@code configs} if they dont exist in it. Ignore if they already.
   * @param config represents the map of configs supposed to be added to global configs
   * @return calling instance of {@link TestRunner} with added configs if they don't exist
   */
  public TestRunner addConfigs(Map<String, String> config) {
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
    configs.put(key, value);
    return this;
  }

  /**
   * Configures {@code stream} with the TestRunner, adds all the stream specific configs to global job configs.
   * <p>
   * Every stream belongs to a System (here a {@link CollectionStreamSystem}), this utility also registers the system with
   * {@link TestRunner} if not registered already. Then it creates and initializes the stream partitions with messages for
   * the registered System
   * <p>
   * @param stream represents the stream that is supposed to be configured with {@link TestRunner}
   * @return calling instance of {@link TestRunner} with {@code stream} configured with it
   */
  public TestRunner addInputStream(CollectionStream stream) {
    Preconditions.checkNotNull(stream);
    Preconditions.checkNotNull(stream.getCollectionStreamSystem());
    CollectionStreamSystem system = stream.getCollectionStreamSystem();
    registerSystem(system);
    system.addInput(stream.getStreamName(), stream.getInitPartitions());
    if (configs.containsKey(TaskConfig.INPUT_STREAMS())) {
      configs.put(TaskConfig.INPUT_STREAMS(),
          configs.get(TaskConfig.INPUT_STREAMS()).concat("," + system.getSystemName() + "." + stream.getStreamName()));
    } else {
      configs.put(TaskConfig.INPUT_STREAMS(), system.getSystemName() + "." + stream.getStreamName());
    }
    stream.getStreamConfig().forEach((key, val) -> {
        configs.putIfAbsent((String) key, (String) val);
      });
    return this;
  }

  /**
   * Configures {@code stream} with the TestRunner, adds all the stream specific configs to global job configs.
   * <p>
   * Every stream belongs to a System (here a {@link CollectionStreamSystem}), this utility also registers the system with
   * {@link TestRunner} if not registered already. Then it creates the stream partitions with the registered System
   * <p>
   * @param stream represents the stream that is supposed to be configured with {@link TestRunner}
   * @return calling instance of {@link TestRunner} with {@code stream} configured with it
   */
  public TestRunner addOutputStream(CollectionStream stream) {
    Preconditions.checkNotNull(stream);
    CollectionStreamSystem system = stream.getCollectionStreamSystem();
    registerSystem(system);
    system.addOutput(stream.getStreamName(), stream.getInitPartitions().size());
    configs.putAll(stream.getStreamConfig());
    return this;
  }

  /**
   * Utility to run a test configured using TestRunner
   * @throws Exception when both {@code app} and {@code taskClass} are null
   */
  public void run() throws Exception {
    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    if (app == null) {
      runner.runTask();
      runner.waitForFinish();
    } else if (app != null) {
      runner.run(app);
      runner.waitForFinish();
    } else {
      throw new Exception("TestRunner should use either run for Low Level Task api or High Level Application Api");
    }
  }
}
