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
package org.apache.samza.storage;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import scala.collection.JavaConversions;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.samza.SamzaException;
import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.RunLoop;
import org.apache.samza.container.RunLoopTask;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.container.TaskInstanceMetrics;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.JobContext;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeManager;
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemConsumers;
import org.apache.samza.system.SystemConsumersMetrics;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.chooser.DefaultChooser;
import org.apache.samza.system.chooser.MessageChooser;
import org.apache.samza.system.chooser.RoundRobinChooserFactory;
import org.apache.samza.table.utils.SerdeUtils;
import org.apache.samza.task.TaskInstanceCollector;
import org.apache.samza.util.Clock;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.ScalaJavaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SideInputsManager {
  private static final Logger LOG = LoggerFactory.getLogger(SideInputsManager.class);

  private static final String SIDE_INPUTS_THREAD_NAME = "SideInputs Thread";
  // We use a prefix to differentiate the SystemConsumersMetrics for sideInputs from the ones in SamzaContainer
  private static final String SIDE_INPUTS_METRICS_PREFIX = "side-inputs-";

  // Timeout with which sideinput thread checks for exceptions and for whether SSPs as caught up
  private static final int SIDE_INPUT_CHECK_TIMEOUT_SECONDS = 10;
  private static final int SIDE_INPUT_SHUTDOWN_TIMEOUT_SECONDS = 60;
  private static final int DEFAULT_SIDE_INPUT_ELASTICITY_FACTOR = 1;

  private final SamzaContainerMetrics samzaContainerMetrics;
  private final Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics;
  private final Config config;

  /* Sideinput related parameters */
  private final boolean hasSideInputs;
  private final Map<TaskName, Map<String, StorageEngine>> sideInputStores;
  // side inputs indexed first by task, then store name
  private final Map<TaskName, Map<String, Set<SystemStreamPartition>>> taskSideInputStoreSSPs;
  private final Set<String> sideInputStoreNames;
  private final Map<SystemStreamPartition, TaskSideInputHandler> sspSideInputHandlers;
  private SystemConsumers sideInputSystemConsumers;

  // Used by the sideInput-read thread to signal to the main thread. Map's contents are mutated.
  private final Map<TaskName, CountDownLatch> sideInputTaskLatches;
  private final ExecutorService sideInputsExecutor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat(SIDE_INPUTS_THREAD_NAME).build());
  private RunLoop sideInputRunLoop; // created in start()

  private volatile boolean shouldShutdown = false;
  private volatile Throwable sideInputException = null;

  public SideInputsManager(Map<String, Set<SystemStream>> sideInputSystemStreams,
      Map<String, SystemFactory> systemFactories,
      Map<String, SystemStream> changelogSystemStreams,
      Map<String, SystemStream> activeTaskChangelogSystemStreams,
      Map<String, StorageEngineFactory<Object, Object>> storageEngineFactories,
      Set<Path> storeDirectoryPaths,
      ContainerModel containerModel, JobContext jobContext, ContainerContext containerContext,
      SamzaContainerMetrics samzaContainerMetrics,
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, TaskInstanceCollector> taskInstanceCollectors,
      StreamMetadataCache streamMetadataCache,
      SystemAdmins systemAdmins,
      SerdeManager serdeManager, Map<String, Serde<Object>> serdes,
      StorageManagerUtil storageManagerUtil,
      File loggedStoreBaseDirectory, File nonLoggedStoreBaseDirectory,
      Config config, Clock clock) {
    this.taskSideInputStoreSSPs = getTaskSideInputSSPs(sideInputSystemStreams, changelogSystemStreams, containerModel);
    this.sideInputStoreNames = ContainerStorageManagerUtil.getSideInputStoreNames(
        sideInputSystemStreams, changelogSystemStreams, containerModel);
    this.sideInputTaskLatches = new HashMap<>();
    this.hasSideInputs = this.taskSideInputStoreSSPs.values().stream()
        .flatMap(m -> m.values().stream())
        .flatMap(Collection::stream)
        .findAny()
        .isPresent();

    this.taskInstanceMetrics = taskInstanceMetrics;
    this.samzaContainerMetrics = samzaContainerMetrics;
    this.config = config;

    // create side input taskStores for all tasks in the containerModel and each store in storageEngineFactories
    this.sideInputStores = ContainerStorageManagerUtil.createTaskStores(
        sideInputStoreNames, storageEngineFactories, sideInputStoreNames,
        activeTaskChangelogSystemStreams, storeDirectoryPaths,
        containerModel, jobContext, containerContext, serdes,
        taskInstanceMetrics, taskInstanceCollectors, storageManagerUtil,
        loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory, config);

    this.sspSideInputHandlers = createSideInputHandlers(hasSideInputs, sideInputStores, taskSideInputStoreSSPs,
        sideInputTaskLatches, taskInstanceMetrics, containerModel, streamMetadataCache, systemAdmins, serdes,
        loggedStoreBaseDirectory, nonLoggedStoreBaseDirectory, config, clock
    );

    // create SystemConsumers for consuming from taskSideInputSSPs, if sideInputs are being used
    if (this.hasSideInputs) {
      Set<SystemStream> containerSideInputSystemStreams = this.taskSideInputStoreSSPs.values().stream()
          .flatMap(map -> map.values().stream())
          .flatMap(Set::stream)
          .map(SystemStreamPartition::getSystemStream)
          .collect(Collectors.toSet());

      Set<String> containerSideInputSystems = containerSideInputSystemStreams.stream()
          .map(SystemStream::getSystem)
          .collect(Collectors.toSet());

      // create sideInput consumers indexed by systemName
      // Mapping from storeSystemNames to SystemConsumers
      Map<String, SystemConsumer> sideInputConsumers =
          ContainerStorageManagerUtil.createSystemConsumers(containerSideInputSystems, systemFactories,
              samzaContainerMetrics.registry(), config);

      scala.collection.immutable.Map<SystemStream, SystemStreamMetadata> inputStreamMetadata =
          streamMetadataCache.getStreamMetadata(JavaConversions.asScalaSet(containerSideInputSystemStreams).toSet(), false);

      // we use the same registry as samza-container-metrics
      SystemConsumersMetrics sideInputSystemConsumersMetrics =
          new SystemConsumersMetrics(samzaContainerMetrics.registry(), SIDE_INPUTS_METRICS_PREFIX);

      MessageChooser chooser = DefaultChooser.apply(inputStreamMetadata, new RoundRobinChooserFactory(), config,
          sideInputSystemConsumersMetrics.registry(), systemAdmins);

      ApplicationConfig applicationConfig = new ApplicationConfig(config);

      this.sideInputSystemConsumers =
          new SystemConsumers(chooser, ScalaJavaUtil.toScalaMap(sideInputConsumers), systemAdmins, serdeManager,
              sideInputSystemConsumersMetrics, SystemConsumers.DEFAULT_NO_NEW_MESSAGES_TIMEOUT(),
              SystemConsumers.DEFAULT_DROP_SERIALIZATION_ERROR(),
              TaskConfig.DEFAULT_POLL_INTERVAL_MS, ScalaJavaUtil.toScalaFunction(() -> System.nanoTime()),
              JobConfig.DEFAULT_JOB_ELASTICITY_FACTOR, applicationConfig.getRunId());
    }
  }

  // read sideInputs until all sideInputStreams are caught up, then return
  public void start() {
    if (this.hasSideInputs) {
      LOG.info("SideInput Restore started");

      // initialize the sideInputStorageManagers
      this.sspSideInputHandlers.values().forEach(TaskSideInputHandler::init);

      Map<TaskName, TaskSideInputHandler> taskSideInputHandlers = this.sspSideInputHandlers.values().stream()
          .distinct()
          .collect(Collectors.toMap(TaskSideInputHandler::getTaskName, Function.identity()));

      Map<TaskName, TaskInstanceMetrics> sideInputTaskMetrics = new HashMap<>();
      Map<TaskName, RunLoopTask> sideInputTasks = new HashMap<>();
      this.taskSideInputStoreSSPs.forEach((taskName, storesToSSPs) -> {
        Set<SystemStreamPartition> taskSSPs = this.taskSideInputStoreSSPs.get(taskName).values().stream()
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

        if (!taskSSPs.isEmpty()) {
          String sideInputSource = SIDE_INPUTS_METRICS_PREFIX + this.taskInstanceMetrics.get(taskName).source();
          TaskInstanceMetrics sideInputMetrics = new TaskInstanceMetrics(
              sideInputSource, this.taskInstanceMetrics.get(taskName).registry(), SIDE_INPUTS_METRICS_PREFIX);
          sideInputTaskMetrics.put(taskName, sideInputMetrics);

          RunLoopTask sideInputTask = new SideInputTask(taskName, taskSSPs,
              taskSideInputHandlers.get(taskName), sideInputTaskMetrics.get(taskName));
          sideInputTasks.put(taskName, sideInputTask);
        }
      });

      // register all sideInput SSPs with the consumers
      for (SystemStreamPartition ssp : this.sspSideInputHandlers.keySet()) {
        String startingOffset = this.sspSideInputHandlers.get(ssp).getStartingOffset(ssp);

        if (startingOffset == null) {
          throw new SamzaException(
              "No starting offset could be obtained for SideInput SystemStreamPartition : " + ssp + ". Consumer cannot start.");
        }

        // register startingOffset with the sysConsumer and register a metric for it
        sideInputSystemConsumers.register(ssp, startingOffset);
        taskInstanceMetrics.get(this.sspSideInputHandlers.get(ssp).getTaskName()).addOffsetGauge(
            ssp, ScalaJavaUtil.toScalaFunction(() -> this.sspSideInputHandlers.get(ssp).getLastProcessedOffset(ssp)));
        sideInputTaskMetrics.get(this.sspSideInputHandlers.get(ssp).getTaskName()).addOffsetGauge(
            ssp, ScalaJavaUtil.toScalaFunction(() -> this.sspSideInputHandlers.get(ssp).getLastProcessedOffset(ssp)));
      }

      // start the systemConsumers for consuming input
      this.sideInputSystemConsumers.start();

      TaskConfig taskConfig = new TaskConfig(this.config);
      SamzaContainerMetrics sideInputContainerMetrics =
          new SamzaContainerMetrics(SIDE_INPUTS_METRICS_PREFIX + this.samzaContainerMetrics.source(),
              this.samzaContainerMetrics.registry(), SIDE_INPUTS_METRICS_PREFIX);

      final ApplicationConfig applicationConfig = new ApplicationConfig(config);

      this.sideInputRunLoop = new RunLoop(sideInputTasks,
          null, // all operations are executed in the main runloop thread
          this.sideInputSystemConsumers,
          1, // single message in flight per task
          -1, // no windowing
          taskConfig.getCommitMs(),
          taskConfig.getCallbackTimeoutMs(),
          taskConfig.getDrainCallbackTimeoutMs(),
          // TODO consolidate these container configs SAMZA-2275
          this.config.getLong("container.disk.quota.delay.max.ms", TimeUnit.SECONDS.toMillis(1)),
          taskConfig.getMaxIdleMs(),
          sideInputContainerMetrics,
          System::nanoTime,
          false,
          DEFAULT_SIDE_INPUT_ELASTICITY_FACTOR,
          applicationConfig.getRunId(),
          ApplicationUtil.isHighLevelApiJob(config)
      ); // commit must be synchronous to ensure integrity of state flush

      try {
        sideInputsExecutor.submit(() -> {
          try {
            sideInputRunLoop.run();
          } catch (Exception e) {
            LOG.error("Exception in reading sideInputs", e);
            sideInputException = e;
          }
        });

        // Make the main thread wait until all sideInputs have been caughtup or an exception was thrown
        while (!shouldShutdown && sideInputException == null &&
            !awaitSideInputTasks(sideInputTaskLatches)) {
          LOG.debug("Waiting for SideInput bootstrap to complete");
        }

        if (sideInputException != null) { // Throw exception if there was an exception in catching-up sideInputs
          throw new SamzaException("Exception in restoring sideInputs", sideInputException);
        }

      } catch (InterruptedException e) {
        LOG.warn("Received an interrupt during side inputs store restoration."
            + " Exiting prematurely without completing store restore.");
        /*
         * We want to stop side input restoration and rethrow the exception upstream. Container should handle the
         * interrupt exception and shutdown the components and cleaning up the resource. We don't want to clean up the
         * resources prematurely here.
         */
        shouldShutdown = true; // todo: should we cancel the flush future right away or wait for container to handle it as part of shutdown sequence?
        throw new SamzaException("Side inputs read was interrupted", e);
      }

      LOG.info("SideInput Restore complete");
    }
  }

  public Map<TaskName, Map<String, StorageEngine>> getSideInputStores() {
    return ImmutableMap.copyOf(this.sideInputStores);
  }

  public void shutdown() {
    this.shouldShutdown = true;

    // stop all side input consumers and stores
    if (this.hasSideInputs) {
      this.sideInputRunLoop.shutdown();
      this.sideInputsExecutor.shutdown();
      try {
        this.sideInputsExecutor.awaitTermination(SIDE_INPUT_SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        throw new SamzaException("Exception while shutting down sideInputs", e);
      }

      this.sideInputSystemConsumers.stop();

      // stop all side input handlers -- this will perform one last flush on the KV stores, and write the offset file
      this.sspSideInputHandlers.values().forEach(TaskSideInputHandler::stop);
    }
  }

  /**
   * Add all sideInputs to a map of maps, indexed first by taskName, then by sideInput store name.
   *
   * @param sideInputSystemStreams the map of store to sideInput system stream
   * @param changelogSystemStreams the map of store to changelog system stream
   * @param containerModel the containerModel to use
   * @return taskSideInputSSPs map
   */
  @VisibleForTesting
  static Map<TaskName, Map<String, Set<SystemStreamPartition>>> getTaskSideInputSSPs(
      Map<String, Set<SystemStream>> sideInputSystemStreams,
      Map<String, SystemStream> changelogSystemStreams,
      ContainerModel containerModel) {
    Map<TaskName, Map<String, Set<SystemStreamPartition>>> taskSideInputSSPs = new HashMap<>();

    containerModel.getTasks().forEach((taskName, taskModel) -> {
      taskSideInputSSPs.putIfAbsent(taskName, new HashMap<>());
      sideInputSystemStreams.keySet().forEach(storeName -> {
        Set<SystemStreamPartition> taskSideInputs = taskModel.getSystemStreamPartitions().stream()
            .filter(ssp -> sideInputSystemStreams.get(storeName).contains(ssp.getSystemStream()))
            .collect(Collectors.toSet());
        taskSideInputSSPs.get(taskName).put(storeName, taskSideInputs);
      });
    });

    ContainerStorageManagerUtil.getTasks(containerModel, TaskMode.Standby).forEach((taskName, taskModel) -> {
      taskSideInputSSPs.putIfAbsent(taskName, new HashMap<>());
      changelogSystemStreams.forEach((storeName, systemStream) -> {
        SystemStreamPartition ssp = new SystemStreamPartition(systemStream, taskModel.getChangelogPartition());
        taskSideInputSSPs.get(taskName).put(storeName, Collections.singleton(ssp));
      });
    });

    return taskSideInputSSPs;
  }

  // Create task sideInput storage managers, one per task, index by the SSP they are responsible for consuming
  // Mutates (creates and adds to) sideInputTaskLatches.
  private static Map<SystemStreamPartition, TaskSideInputHandler> createSideInputHandlers(
      boolean hasSideInputs, Map<TaskName, Map<String, StorageEngine>> sideInputStores,
      Map<TaskName, Map<String, Set<SystemStreamPartition>>> taskSideInputStoreSSPs,
      Map<TaskName, CountDownLatch> sideInputTaskLatches, Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      ContainerModel containerModel, StreamMetadataCache streamMetadataCache, SystemAdmins systemAdmins,
      Map<String, Serde<Object>> serdes, File loggedStoreBaseDirectory, File nonLoggedStoreBaseDirectory,
      Config config, Clock clock) {
    // creating sideInput store processors, one per store per task
    Map<TaskName, Map<String, SideInputsProcessor>> taskSideInputProcessors =
        createSideInputProcessors(taskInstanceMetrics, taskSideInputStoreSSPs,
            containerModel, serdes, new StorageConfig(config));

    Map<SystemStreamPartition, TaskSideInputHandler> handlers = new HashMap<>();

    if (hasSideInputs) {
      containerModel.getTasks().forEach((taskName, taskModel) -> {

        Map<String, StorageEngine> taskSideInputStores = sideInputStores.get(taskName);
        Map<String, Set<SystemStreamPartition>> sideInputStoresToSSPs = new HashMap<>();
        boolean taskHasSideInputs = false;
        for (String storeName : taskSideInputStores.keySet()) {
          Set<SystemStreamPartition> storeSSPs = taskSideInputStoreSSPs.get(taskName).get(storeName);
          taskHasSideInputs = taskHasSideInputs || !storeSSPs.isEmpty();
          sideInputStoresToSSPs.put(storeName, storeSSPs);
        }

        if (taskHasSideInputs) {
          CountDownLatch taskCountDownLatch = new CountDownLatch(1);
          sideInputTaskLatches.put(taskName, taskCountDownLatch);

          TaskSideInputHandler taskSideInputHandler = new TaskSideInputHandler(taskName,
              taskModel.getTaskMode(),
              loggedStoreBaseDirectory,
              taskSideInputStores,
              sideInputStoresToSSPs,
              taskSideInputProcessors.get(taskName),
              systemAdmins,
              streamMetadataCache,
              taskCountDownLatch,
              clock);

          sideInputStoresToSSPs.values().stream().flatMap(Set::stream).forEach(ssp -> {
            handlers.put(ssp, taskSideInputHandler);
          });

          LOG.info("Created TaskSideInputHandler for task {}, taskSideInputStores {} and loggedStoreBaseDirectory {}",
              taskName, taskSideInputStores, loggedStoreBaseDirectory);
        }
      });
    }
    return handlers;
  }

  // Create sideInput store processors, one per store per task
  private static Map<TaskName, Map<String, SideInputsProcessor>> createSideInputProcessors(
      Map<TaskName, TaskInstanceMetrics> taskInstanceMetrics,
      Map<TaskName, Map<String, Set<SystemStreamPartition>>> taskSideInputStoreSSPs,
      ContainerModel containerModel, Map<String, Serde<Object>> serdes, StorageConfig config) {

    Map<TaskName, Map<String, SideInputsProcessor>> sideInputStoresToProcessors = new HashMap<>();
    containerModel.getTasks().forEach((taskName, taskModel) -> {
      sideInputStoresToProcessors.put(taskName, new HashMap<>());
      TaskMode taskMode = taskModel.getTaskMode();

      for (String storeName : taskSideInputStoreSSPs.get(taskName).keySet()) {
        SideInputsProcessor sideInputsProcessor;
        Optional<String> sideInputsProcessorSerializedInstance =
            config.getSideInputsProcessorSerializedInstance(storeName);

        if (sideInputsProcessorSerializedInstance.isPresent()) {

          sideInputsProcessor = SerdeUtils.deserialize("Side Inputs Processor", sideInputsProcessorSerializedInstance.get());
          LOG.info("Using serialized side-inputs-processor for store: {}, task: {}", storeName, taskName);

        } else if (config.getSideInputsProcessorFactory(storeName).isPresent()) {
          String sideInputsProcessorFactoryClassName = config.getSideInputsProcessorFactory(storeName).get();
          SideInputsProcessorFactory sideInputsProcessorFactory =
              ReflectionUtil.getObj(sideInputsProcessorFactoryClassName, SideInputsProcessorFactory.class);
          sideInputsProcessor = sideInputsProcessorFactory.getSideInputsProcessor(
              config, taskInstanceMetrics.get(taskName).registry());
          LOG.info("Using side-inputs-processor from factory: {} for store: {}, task: {}",
              config.getSideInputsProcessorFactory(storeName).get(), storeName, taskName);

        } else {
          // if this is a active-task with a side-input store but no sideinput-processor-factory defined in config,
          // we rely on upstream validations to fail the deploy

          // if this is a standby-task and the store is a non-side-input changelog store
          // we creating identity sideInputProcessor for stores of standbyTasks
          // have to use the right serde because the sideInput stores are created

          Serde keySerde = serdes.get(config.getStorageKeySerde(storeName)
              .orElseThrow(() -> new SamzaException("Could not find storage key serde for store: " + storeName)));
          Serde msgSerde = serdes.get(config.getStorageMsgSerde(storeName)
              .orElseThrow(() -> new SamzaException("Could not find storage msg serde for store: " + storeName)));
          sideInputsProcessor = new SideInputsProcessor() {
            @Override
            public Collection<Entry<?, ?>> process(IncomingMessageEnvelope message, KeyValueStore store) {
              // Ignore message if the key is null
              if (message.getKey() == null) {
                return ImmutableList.of();
              } else {
                // Skip serde if the message is null
                return ImmutableList.of(new Entry<>(keySerde.fromBytes((byte[]) message.getKey()),
                    message.getMessage() == null ? null : msgSerde.fromBytes((byte[]) message.getMessage())));
              }
            }
          };
          LOG.info("Using identity side-inputs-processor for store: {}, task: {}", storeName, taskName);
        }

        sideInputStoresToProcessors.get(taskName).put(storeName, sideInputsProcessor);
      }
    });

    return sideInputStoresToProcessors;
  }

  /**
   * Waits for all side input tasks to catch up until a timeout.
   *
   * @return False if waiting on any latch timed out, true otherwise
   *
   * @throws InterruptedException if waiting any of the latches is interrupted
   */
  private static boolean awaitSideInputTasks(Map<TaskName, CountDownLatch> sideInputTaskLatches) throws InterruptedException {
    long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(SIDE_INPUT_CHECK_TIMEOUT_SECONDS);
    for (CountDownLatch latch : sideInputTaskLatches.values()) {
      long remainingMillisToWait = endTime - System.currentTimeMillis();
      if (remainingMillisToWait <= 0 || !latch.await(remainingMillisToWait, TimeUnit.MILLISECONDS)) {
        return false;
      }
    }
    return true;
  }
}
