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
package org.apache.samza.clustermanager;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementMetadataStore;
import org.apache.samza.application.ApplicationUtil;
import org.apache.samza.classloader.IsolatingClassLoaderFactory;
import org.apache.samza.clustermanager.container.placement.ContainerPlacementRequestAllocator;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.InputStreamsDiscoveredException;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.MetadataResourceUtil;
import org.apache.samza.coordinator.PartitionChangeException;
import org.apache.samza.coordinator.StreamPartitionCountMonitor;
import org.apache.samza.coordinator.StreamRegexMonitor;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.JobModelUtil;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.JmxServer;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.storage.ChangelogStreamManager;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.ConfigUtil;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.DiagnosticsUtil;
import org.apache.samza.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements a JobCoordinator that is completely independent of the underlying cluster
 * manager system. This {@link ClusterBasedJobCoordinator} handles functionality common
 * to both Yarn and Mesos. It takes care of
 *  1. Requesting resources from an underlying {@link ClusterResourceManager}.
 *  2. Ensuring that placement of processors to resources happens (as per whether host affinity
 *  is configured or not).
 *
 *  Any offer based cluster management system that must integrate with Samza will merely
 *  implement a {@link ResourceManagerFactory} and a {@link ClusterResourceManager}.
 *
 *  This class is not thread-safe. For safe access in multi-threaded context, invocations
 *  should be synchronized by the callers.
 *
 * TODO:
 * 1. Refactor ClusterResourceManager to also handle process liveness, process start
 * callbacks
 * 2. Refactor the JobModelReader to be an interface.
 * 3. Make ClusterBasedJobCoordinator implement the JobCoordinator API as in SAMZA-881.
 * 4. Refactor UI state variables.
 * 5. Unit tests.
 * 6. Document newly added configs.
 */
public class ClusterBasedJobCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterBasedJobCoordinator.class);
  private final static String METRICS_SOURCE_NAME = "ApplicationMaster";

  private final Config config;

  /**
   * State to track container failures, host-processor mappings
   */
  private final SamzaApplicationState state;

  //even though some of these can be converted to local variables, it will not be the case
  //as we add more methods to the JobCoordinator and completely implement SAMZA-881.

  /**
   * Handles callback for allocated containers, failed containers.
   */
  private final ContainerProcessManager containerProcessManager;

  /**
   * A JobModelManager to return and refresh the {@link org.apache.samza.job.model.JobModel} when required.
   */
  private final JobModelManager jobModelManager;

  /**
   * A ChangelogStreamManager to handle creation of changelog stream and map changelog stream partitions
   */
  private final ChangelogStreamManager changelogStreamManager;

  /*
   * The interval for polling the Task Manager for shutdown.
   */
  private final long jobCoordinatorSleepInterval;

  /*
   * Config specifies if a Jmx server should be started on this Job Coordinator
   */
  private final boolean isJmxEnabled;

  /**
   * Internal boolean to check if the job coordinator has already been started.
   */
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  /**
   * A boolean variable indicating whether the job has durable state stores in the configuration
   */
  private final boolean hasDurableStores;

  /**
   * The input topic partition count monitor
   */
  private final Optional<StreamPartitionCountMonitor> partitionMonitor;

  /**
   * The input stream regex monitor
   */
  private final Optional<StreamRegexMonitor> inputStreamRegexMonitor;

  /**
   * Container placement request dispatcher and metastore reader/writer
   */
  private final ContainerPlacementMetadataStore containerPlacementMetadataStore;
  private final ContainerPlacementRequestAllocator containerPlacementRequestAllocator;
  // Container Placement thread that reads requests from metastore
  private final Thread containerPlacementRequestAllocatorThread;

  /**
   * Metrics to track stats around container failures, needed containers etc.
   */
  private final MetricsRegistryMap metrics;
  private final MetadataStore metadataStore;

  private final SystemAdmins systemAdmins;

  /**
   * Internal variable for the instance of {@link JmxServer}
   */
  private JmxServer jmxServer;

  /**
   * Variable to keep the callback exception
   */
  volatile private Exception coordinatorException = null;

  /**
   * Creates a new ClusterBasedJobCoordinator instance.
   * Invoke run() to actually run the job coordinator.
   *
   * @param metrics the registry for reporting metrics.
   * @param metadataStore metadata store to hold metadata.
   * @param fullJobConfig full job config.
   */
  public ClusterBasedJobCoordinator(MetricsRegistryMap metrics, MetadataStore metadataStore, Config fullJobConfig) {
    this.metrics = metrics;
    this.metadataStore = metadataStore;
    this.config = fullJobConfig;
    // build a JobModelManager and ChangelogStreamManager and perform partition assignments.
    this.changelogStreamManager = new ChangelogStreamManager(
        new NamespaceAwareCoordinatorStreamStore(metadataStore, SetChangelogMapping.TYPE));
    this.jobModelManager =
        JobModelManager.apply(config, changelogStreamManager.readPartitionMapping(), metadataStore, metrics);

    this.hasDurableStores = new StorageConfig(config).hasDurableStores();
    this.state = new SamzaApplicationState(jobModelManager);
    // The systemAdmins should be started before partitionMonitor can be used. And it should be stopped when this coordinator is stopped.
    this.systemAdmins = new SystemAdmins(config);
    this.partitionMonitor = getPartitionCountMonitor(config, systemAdmins);

    Set<SystemStream> inputSystemStreams = JobModelUtil.getSystemStreams(jobModelManager.jobModel());
    this.inputStreamRegexMonitor = getInputRegexMonitor(config, systemAdmins, inputSystemStreams);

    ClusterManagerConfig clusterManagerConfig = new ClusterManagerConfig(config);
    this.isJmxEnabled = clusterManagerConfig.getJmxEnabledOnJobCoordinator();
    this.jobCoordinatorSleepInterval = clusterManagerConfig.getJobCoordinatorSleepInterval();

    // build metastore for container placement messages
    containerPlacementMetadataStore = new ContainerPlacementMetadataStore(metadataStore);

    // build a container process Manager
    containerProcessManager = createContainerProcessManager();

    // build utils related to container placements
    containerPlacementRequestAllocator =
        new ContainerPlacementRequestAllocator(containerPlacementMetadataStore, containerProcessManager,
            new ApplicationConfig(config));
    this.containerPlacementRequestAllocatorThread =
        new Thread(containerPlacementRequestAllocator, "Samza-" + ContainerPlacementRequestAllocator.class.getSimpleName());
  }

  /**
   * Starts the JobCoordinator.
   */
  public void run() {
    if (!isStarted.compareAndSet(false, true)) {
      LOG.warn("Attempting to start an already started job coordinator. ");
      return;
    }
    // set up JmxServer (if jmx is enabled)
    if (isJmxEnabled) {
      jmxServer = new JmxServer();
      state.jmxUrl = jmxServer.getJmxUrl();
      state.jmxTunnelingUrl = jmxServer.getTunnelingJmxUrl();
    } else {
      jmxServer = null;
    }

    try {
      // initialize JobCoordinator state
      LOG.info("Starting cluster based job coordinator");

      // write the diagnostics metadata file
      String jobName = new JobConfig(config).getName().get();
      String jobId = new JobConfig(config).getJobId();
      Optional<String> execEnvContainerId = Optional.ofNullable(System.getenv("CONTAINER_ID"));
      DiagnosticsUtil.writeMetadataFile(jobName, jobId, METRICS_SOURCE_NAME, execEnvContainerId, config);

      //create necessary checkpoint and changelog streams, if not created
      JobModel jobModel = jobModelManager.jobModel();
      MetadataResourceUtil metadataResourceUtil = new MetadataResourceUtil(jobModel, this.metrics, config);
      metadataResourceUtil.createResources();

      // fan out the startpoints
      StartpointManager startpointManager = createStartpointManager();
      startpointManager.start();
      try {
        if () {
          startpointManager.fanOut(JobModelUtil.getTaskToSystemStreamPartitions(jobModel));
        }
      } finally {
        startpointManager.stop();
      }

      // Remap changelog partitions to tasks
      Map<TaskName, Integer> prevPartitionMappings = changelogStreamManager.readPartitionMapping();

      Map<TaskName, Integer> taskPartitionMappings = new HashMap<>();
      Map<String, ContainerModel> containers = jobModel.getContainers();
      for (ContainerModel containerModel : containers.values()) {
        for (TaskModel taskModel : containerModel.getTasks().values()) {
          taskPartitionMappings.put(taskModel.getTaskName(), taskModel.getChangelogPartition().getPartitionId());
        }
      }

      changelogStreamManager.updatePartitionMapping(prevPartitionMappings, taskPartitionMappings);

      containerProcessManager.start();
      systemAdmins.start();
      partitionMonitor.ifPresent(StreamPartitionCountMonitor::start);
      inputStreamRegexMonitor.ifPresent(StreamRegexMonitor::start);

      // containerPlacementRequestAllocator thread has to start after the cpm is started
      LOG.info("Starting the container placement handler thread");
      containerPlacementMetadataStore.start();
      containerPlacementRequestAllocatorThread.start();

      boolean isInterrupted = false;

      while (!containerProcessManager.shouldShutdown()
          && !checkAndThrowException()
          && !isInterrupted
          && checkcontainerPlacementRequestAllocatorThreadIsAlive()) {
        try {
          Thread.sleep(jobCoordinatorSleepInterval);
        } catch (InterruptedException e) {
          isInterrupted = true;
          LOG.error("Interrupted in job coordinator loop", e);
          Thread.currentThread().interrupt();
        }
      }
    } catch (Throwable e) {
      LOG.error("Exception thrown in the JobCoordinator loop", e);
      throw new SamzaException(e);
    } finally {
      onShutDown();
    }
  }

  private boolean checkAndThrowException() throws Exception {
    if (coordinatorException != null) {
      throw coordinatorException;
    }
    return false;
  }

  private boolean checkcontainerPlacementRequestAllocatorThreadIsAlive() {
    if (containerPlacementRequestAllocatorThread.isAlive()) {
      return true;
    }
    LOG.info("{} thread is dead issuing a shutdown", containerPlacementRequestAllocatorThread.getName());
    return false;
  }

  /**
   * Stops all components of the JobCoordinator.
   */
  private void onShutDown() {
    try {
      partitionMonitor.ifPresent(StreamPartitionCountMonitor::stop);
      inputStreamRegexMonitor.ifPresent(StreamRegexMonitor::stop);
      systemAdmins.stop();
      shutDowncontainerPlacementRequestAllocatorAndUtils();
      containerProcessManager.stop();
      metadataStore.close();
    } catch (Throwable e) {
      LOG.error("Exception while stopping cluster based job coordinator", e);
    }
    LOG.info("Stopped cluster based job coordinator");

    if (jmxServer != null) {
      try {
        jmxServer.stop();
        LOG.info("Stopped Jmx Server");
      } catch (Throwable e) {
        LOG.error("Exception while stopping jmx server", e);
      }
    }
  }

  private void shutDowncontainerPlacementRequestAllocatorAndUtils() {
    // Shutdown container placement handler
    containerPlacementRequestAllocator.stop();
    try {
      containerPlacementRequestAllocatorThread.join();
      LOG.info("Stopped container placement handler thread");
      containerPlacementMetadataStore.stop();
    } catch (InterruptedException ie) {
      LOG.error("Container Placement handler thread join threw an interrupted exception", ie);
      Thread.currentThread().interrupt();
    }
  }

  private Optional<StreamPartitionCountMonitor> getPartitionCountMonitor(Config config, SystemAdmins systemAdmins) {
    StreamMetadataCache streamMetadata = new StreamMetadataCache(systemAdmins, 0, SystemClock.instance());
    Set<SystemStream> inputStreamsToMonitor = new TaskConfig(config).getAllInputStreams();
    if (inputStreamsToMonitor.isEmpty()) {
      throw new SamzaException("Input streams to a job can not be empty.");
    }

    return Optional.of(new StreamPartitionCountMonitor(inputStreamsToMonitor, streamMetadata, metrics,
        new JobConfig(config).getMonitorPartitionChangeFrequency(), streamsChanged -> {
      // Fail the jobs with durable state store. Otherwise, application state.status remains UNDEFINED s.t. YARN job will be restarted
        if (hasDurableStores) {
          LOG.error("Input topic partition count changed in a job with durable state. Failing the job. " +
              "Changed topics: {}", streamsChanged.toString());
          state.status = SamzaApplicationState.SamzaAppStatus.FAILED;
        }
        coordinatorException = new PartitionChangeException("Input topic partition count changes detected for topics: " + streamsChanged.toString());
      }));
  }

  private Optional<StreamRegexMonitor> getInputRegexMonitor(Config config, SystemAdmins systemAdmins, Set<SystemStream> inputStreamsToMonitor) {
    JobConfig jobConfig = new JobConfig(config);

    // if input regex monitor is not enabled return empty
    if (jobConfig.getMonitorRegexDisabled()) {
      LOG.info("StreamRegexMonitor is disabled.");
      return Optional.empty();
    }

    StreamMetadataCache streamMetadata = new StreamMetadataCache(systemAdmins, 0, SystemClock.instance());
    if (inputStreamsToMonitor.isEmpty()) {
      throw new SamzaException("Input streams to a job can not be empty.");
    }

    // First list all rewriters
    Optional<String> rewritersList = jobConfig.getConfigRewriters();

    // if no rewriter is defined, there is nothing to monitor
    if (!rewritersList.isPresent()) {
      LOG.warn("No config rewriters are defined. No StreamRegexMonitor created.");
      return Optional.empty();
    }

    // Compile a map of each input-system to its corresponding input-monitor-regex patterns
    Map<String, Pattern> inputRegexesToMonitor = jobConfig.getMonitorRegexPatternMap(rewritersList.get());

    // if there are no regexes to monitor
    if (inputRegexesToMonitor.isEmpty()) {
      LOG.info("No input regexes are defined. No StreamRegexMonitor created.");
      return Optional.empty();
    }

    return Optional.of(new StreamRegexMonitor(inputStreamsToMonitor, inputRegexesToMonitor, streamMetadata, metrics,
        jobConfig.getMonitorRegexFrequency(), new StreamRegexMonitor.Callback() {
          @Override
          public void onInputStreamsChanged(Set<SystemStream> initialInputSet, Set<SystemStream> newInputStreams,
              Map<String, Pattern> regexesMonitored) {
            if (hasDurableStores) {
              LOG.error("New input system-streams discovered. Failing the job. New input streams: {}" +
                  " Existing input streams: {}", newInputStreams, inputStreamsToMonitor);
              state.status = SamzaApplicationState.SamzaAppStatus.FAILED;
            }
            coordinatorException = new InputStreamsDiscoveredException("New input streams discovered: " + newInputStreams);
          }
        }));
  }

  // The following two methods are package-private and for testing only
  @VisibleForTesting
  SamzaApplicationState.SamzaAppStatus getAppStatus() {
    // make sure to only return a unmodifiable copy of the status variable
    return state.status;
  }

  @VisibleForTesting
  StreamPartitionCountMonitor getPartitionMonitor() {
    return partitionMonitor.get();
  }

  @VisibleForTesting
  StartpointManager createStartpointManager() {
    return new StartpointManager(metadataStore);
  }

  @VisibleForTesting
  ContainerProcessManager createContainerProcessManager() {
    return new ContainerProcessManager(config, state, metrics, containerPlacementMetadataStore);
  }

  /**
   * The entry point for the {@link ClusterBasedJobCoordinator}.
   */
  public static void main(String[] args) {
    boolean dependencyIsolationEnabled = Boolean.parseBoolean(
        System.getenv(ShellCommandConfig.ENV_CLUSTER_BASED_JOB_COORDINATOR_DEPENDENCY_ISOLATION_ENABLED));
    Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
        LOG.error("Uncaught exception in ClusterBasedJobCoordinator::main. Exiting job coordinator", exception);
        System.exit(1);
      });
    if (!dependencyIsolationEnabled) {
      // no isolation enabled, so can just execute runClusterBasedJobCoordinator directly
      runClusterBasedJobCoordinator(args);
    } else {
      runWithClassLoader(new IsolatingClassLoaderFactory().buildClassLoader(), args);
    }
    System.exit(0);
  }

  /**
   * Execute the coordinator using a separate isolated classloader.
   * @param classLoader {@link ClassLoader} to use to load the {@link ClusterBasedJobCoordinator} which will run
   * @param args arguments to pass when running the {@link ClusterBasedJobCoordinator}
   */
  @VisibleForTesting
  static void runWithClassLoader(ClassLoader classLoader, String[] args) {
    // need to use the isolated classloader to load ClusterBasedJobCoordinator and then run using that new class
    Class<?> clusterBasedJobCoordinatorClass;
    try {
      clusterBasedJobCoordinatorClass = classLoader.loadClass(ClusterBasedJobCoordinator.class.getName());
    } catch (ClassNotFoundException e) {
      throw new SamzaException(
          "Isolation was enabled, but unable to find ClusterBasedJobCoordinator in isolated classloader", e);
    }

    // save the current context classloader so it can be reset after finishing the call to runClusterBasedJobCoordinator
    ClassLoader previousContextClassLoader = Thread.currentThread().getContextClassLoader();
    // this is needed because certain libraries (e.g. log4j) use the context classloader
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      executeRunClusterBasedJobCoordinatorForClass(clusterBasedJobCoordinatorClass, args);
    } finally {
      // reset the context class loader; it's good practice, and could be important when running a test suite
      Thread.currentThread().setContextClassLoader(previousContextClassLoader);
    }
  }

  /**
   * Runs the {@link ClusterBasedJobCoordinator#runClusterBasedJobCoordinator(String[])} method of the given
   * {@code clusterBasedJobCoordinatorClass} using reflection.
   * @param clusterBasedJobCoordinatorClass {@link ClusterBasedJobCoordinator} {@link Class} for which to execute
   * {@link ClusterBasedJobCoordinator#runClusterBasedJobCoordinator(String[])}
   * @param args arguments to pass to {@link ClusterBasedJobCoordinator#runClusterBasedJobCoordinator(String[])}
   */
  private static void executeRunClusterBasedJobCoordinatorForClass(Class<?> clusterBasedJobCoordinatorClass,
      String[] args) {
    Method runClusterBasedJobCoordinatorMethod;
    try {
      runClusterBasedJobCoordinatorMethod =
          clusterBasedJobCoordinatorClass.getDeclaredMethod("runClusterBasedJobCoordinator", String[].class);
    } catch (NoSuchMethodException e) {
      throw new SamzaException("Isolation was enabled, but unable to find runClusterBasedJobCoordinator method", e);
    }
    // only sets accessible flag for this Method instance, not other Method instances for runClusterBasedJobCoordinator
    runClusterBasedJobCoordinatorMethod.setAccessible(true);

    try {
      // wrapping args in object array so that args is passed as a single argument to the method
      runClusterBasedJobCoordinatorMethod.invoke(null, new Object[]{args});
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new SamzaException("Exception while executing runClusterBasedJobCoordinator method", e);
    }
  }

  /**
   * This is the actual execution for the {@link ClusterBasedJobCoordinator}. This is separated out from
   * {@link #main(String[])} so that it can be executed directly or from a separate classloader.
   */
  private static void runClusterBasedJobCoordinator(String[] args) {
    final String coordinatorSystemEnv = System.getenv(ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG);
    final String submissionEnv = System.getenv(ShellCommandConfig.ENV_SUBMISSION_CONFIG);

    if (!StringUtils.isBlank(submissionEnv)) {
      Config submissionConfig;
      try {
        //Read and parse the coordinator system config.
        LOG.info("Parsing submission config {}", submissionEnv);
        submissionConfig =
            new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(submissionEnv, Config.class));
        LOG.info("Using the submission config: {}.", submissionConfig);
      } catch (IOException e) {
        LOG.error("Exception while reading submission config", e);
        throw new SamzaException(e);
      }

      ApplicationConfig appConfig = new ApplicationConfig(submissionConfig);

      /*
       * Invoke app.main.class with app.main.args when present.
       * For Beam jobs, app.main.class will be Beam's main class
       * and app.main.args will be Beam's pipeline options.
       */
      if (appConfig.getAppMainClass().isPresent()) {
        String className = appConfig.getAppMainClass().get();
        LOG.info("Invoke main {}", className);
        try {
          Class<?> cls = Class.forName(className);
          Method mainMethod = cls.getMethod("main", String[].class);
          mainMethod.invoke(null, (Object) toArgs(appConfig));
        } catch (Exception e) {
          throw new SamzaException(e);
        }
      } else {
        JobConfig jobConfig = new JobConfig(submissionConfig);

        if (!jobConfig.getConfigLoaderFactory().isPresent()) {
          throw new SamzaException(JobConfig.CONFIG_LOADER_FACTORY + " is required to initialize job coordinator from config loader");
        }

        // load full job config with ConfigLoader
        Config originalConfig = ConfigUtil.loadConfig(submissionConfig);

        JobCoordinatorLaunchUtil.run(ApplicationUtil.fromConfig(originalConfig), originalConfig);
      }

      LOG.info("Finished running ClusterBasedJobCoordinator");
    } else {
      // TODO: Clean this up once SAMZA-2405 is completed when legacy flow is removed.
      Config coordinatorSystemConfig;
      try {
        //Read and parse the coordinator system config.
        LOG.info("Parsing coordinator system config {}", coordinatorSystemEnv);
        coordinatorSystemConfig =
            new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(coordinatorSystemEnv, Config.class));
        LOG.info("Using the coordinator system config: {}.", coordinatorSystemConfig);
      } catch (IOException e) {
        LOG.error("Exception while reading coordinator stream config", e);
        throw new SamzaException(e);
      }
      ClusterBasedJobCoordinator jc = createFromMetadataStore(coordinatorSystemConfig);
      jc.run();
      LOG.info("Finished running ClusterBasedJobCoordinator");
    }
  }

  /**
   * Initialize {@link ClusterBasedJobCoordinator} with coordinator stream config, full job config will be fetched from
   * coordinator stream.
   *
   * @param metadataStoreConfig to initialize {@link MetadataStore}
   * @return {@link ClusterBasedJobCoordinator}
   */
  // TODO SAMZA-2432: Clean this up once SAMZA-2405 is completed when legacy flow is removed.
  @VisibleForTesting
  static ClusterBasedJobCoordinator createFromMetadataStore(Config metadataStoreConfig) {
    MetricsRegistryMap metrics = new MetricsRegistryMap();

    CoordinatorStreamStore coordinatorStreamStore = new CoordinatorStreamStore(metadataStoreConfig, metrics);
    coordinatorStreamStore.init();
    Config config = CoordinatorStreamUtil.readConfigFromCoordinatorStream(coordinatorStreamStore);

    return new ClusterBasedJobCoordinator(metrics, coordinatorStreamStore, config);
  }

  /**
   * Convert Samza config to command line arguments to invoke app.main.class
   *
   * @param config Samza config to convert.
   * @return converted command line arguments.
   */
  @VisibleForTesting
  static String[] toArgs(ApplicationConfig config) {
    List<String> args = new ArrayList<>(config.size() * 2);

    config.forEach((key, value) -> {
        if (key.equals(ApplicationConfig.APP_MAIN_ARGS)) {
          /*
           * Converts native beam pipeline options such as
           * --runner=SamzaRunner --maxSourceParallelism=1024
           */
          args.addAll(Arrays.asList(value.split("\\s")));
        } else {
          /*
           * Converts native Samza configs to config override format such as
           * --config job.name=test
           */
          args.add("--config");
          args.add(String.format("%s=%s", key, value));
        }
      });

    return args.toArray(new String[0]);
  }
}
