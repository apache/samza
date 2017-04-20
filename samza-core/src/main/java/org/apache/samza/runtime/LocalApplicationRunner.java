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

package org.apache.samza.runtime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.coordinator.CoordinationServiceFactory;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.Latch;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifecycleListener;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.ClassLoaderHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in standalone environment
 */
public class LocalApplicationRunner extends AbstractApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(LocalApplicationRunner.class);
  // Latch id that's used for awaiting the init of application before creating the StreamProcessors
  private static final String LATCH_INIT = "init";
  // Latch timeout is set to 10 min
  private static final long LATCH_TIMEOUT_MINUTES = 10;

  private final String uid;
  private final CoordinationUtils coordinationUtils;
  private final List<StreamProcessor> processors = new ArrayList<>();
  private final CountDownLatch latch = new CountDownLatch(1);
  private final AtomicReference<Throwable> throwable = new AtomicReference<>();
  private final Set<String> processorIds = ConcurrentHashMap.newKeySet();

  private ApplicationStatus appStatus = ApplicationStatus.New;

  final class LocalStreamProcessorLifeCycleListener implements StreamProcessorLifecycleListener {
    public final String processorId;

    public LocalStreamProcessorLifeCycleListener(String processorId) {
      this.processorId = processorId;
    }

    @Override
    public void onStart() {
      processorIds.add(processorId);
      if (processorIds.size() == processors.size()) {
        appStatus = ApplicationStatus.Running;
      }
    }

    @Override
    public void onShutdown() {
      processorIds.remove(processorId);
      if (processorIds.isEmpty()) {
        latch.countDown();
      }
    }

    @Override
    public void onFailure(Throwable t) {
      processorIds.remove(processorId);
      throwable.compareAndSet(null, t);
      latch.countDown();
    }
  }

  public LocalApplicationRunner(Config config) throws Exception {
    super(config);
    uid = UUID.randomUUID().toString();
    coordinationUtils = createCoordinationUtils();
  }

  @Override
  public void run(StreamApplication app) {
    try {
      // 1. initialize and plan
      ExecutionPlan plan = getExecutionPlan(app);

      // 2. create the necessary streams
      createStreams(plan.getIntermediateStreams());

      // 3. start the StreamProcessors
      if (plan.getJobConfigs().isEmpty()) {
        throw new SamzaException("No jobs to run.");
      }
      plan.getJobConfigs().forEach(jobConfig -> {
          log.debug("Starting job {} StreamProcessor with config {}", jobConfig.getName(), jobConfig);
          String processorId = getProcessorId(config);
          if (StringUtils.isEmpty(processorId)) {
            throw new SamzaException("processorId has to be defined when running StreamApplication locally.");
          }

          StreamProcessor processor =
              createStreamProcessor(processorId, jobConfig, app, new LocalStreamProcessorLifeCycleListener(processorId));
          processor.start();
          processors.add(processor);
        });

      // 4. block until the processors are done or there is a failure
      latch.await();

      if (throwable.get() != null) {
        appStatus = ApplicationStatus.UnsuccessfulFinish;
        throw throwable.get();
      } else {
        appStatus = ApplicationStatus.SuccessfulFinish;
      }

    } catch (Throwable t) {
      appStatus = ApplicationStatus.UnsuccessfulFinish;
      throw new SamzaException("Failed to run application", t);
    } finally {
      if (coordinationUtils != null) {
        coordinationUtils.reset();
      }
    }
  }

  @Override
  public void kill(StreamApplication streamApp) {
    processors.forEach(StreamProcessor::stop);
  }

  @Override
  public ApplicationStatus status(StreamApplication streamApp) {
    return appStatus;
  }

  /**
   * Create the {@link CoordinationUtils} needed by the application runner.
   * @return an instance of {@link CoordinationUtils}
   * @throws Exception exceptions when creating the instance, such as {@link ClassNotFoundException} and
   * {@link InstantiationException}
   */
  /* package private */ CoordinationUtils createCoordinationUtils() throws Exception {
    ApplicationConfig appConfig = new ApplicationConfig(config);
    String clazz = appConfig.getCoordinationServiceFactoryClass();
    if (StringUtils.isNotEmpty(clazz)) {
      CoordinationServiceFactory factory = ClassLoaderHelper.fromClassName(clazz);
      String groupId = String.format("app-%s-%s", appConfig.getAppName(), appConfig.getAppId());
      return factory.getCoordinationService(groupId, uid, config);
    } else {
      return null;
    }
  }

  /**
   * Create intermediate streams using {@link org.apache.samza.execution.StreamManager}.
   * If {@link CoordinationUtils} is provided, this function will first invoke leader election, and the leader
   * will create the streams. All the runner processes will wait on the latch that is released after the leader finishes
   * stream creation.
   * @param intStreams list of intermediate {@link StreamSpec}s
   * @throws Exception exception for latch timeout
   */
  /* package private */ void createStreams(List<StreamSpec> intStreams) throws Exception {
    if (!intStreams.isEmpty()) {
      if (coordinationUtils != null) {
        Latch initLatch = coordinationUtils.getLatch(1, LATCH_INIT);
        coordinationUtils.getLeaderElector().tryBecomeLeader(() -> {
            getStreamManager().createStreams(intStreams);
            initLatch.countDown();
          });
        initLatch.await(LATCH_TIMEOUT_MINUTES, TimeUnit.MINUTES);
      } else {
        // each application process will try creating the streams, which
        // requires stream creation to be idempotent
        getStreamManager().createStreams(intStreams);
      }
    }
  }

  /**
   * Generates processorId for each {@link StreamProcessor} using the configured {@link ProcessorIdGenerator}
   *
   * @param config Application config
   * @return String that uniquely represents an instance of {@link StreamProcessor} in the current JVM
   */
  /* package private */
  String getProcessorId(Config config) {
    // TODO: This check to be removed after 0.13+
    ApplicationConfig appConfig = new ApplicationConfig(config);
    if (appConfig.getProcessorId() != null) {
      return appConfig.getProcessorId();
    } else if (appConfig.getAppProcessorIdGeneratorClass() != null) {
      ProcessorIdGenerator idGenerator = ClassLoaderHelper.fromClassName(appConfig.getAppProcessorIdGeneratorClass(),
                                                                         ProcessorIdGenerator.class);
      return idGenerator.generateProcessorId(config);
    } else {
      throw new ConfigException(
          String.format("Expected either %s or %s to be configured", ApplicationConfig.PROCESSOR_ID,
              ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS));
    }
  }

  /**
   * Create {@link StreamProcessor} based on {@link StreamApplication} and the config
   * @param config config
   * @param app {@link StreamApplication}
   * @return {@link StreamProcessor]}
   */
  /* package private */
  StreamProcessor createStreamProcessor(
      String processorId,
      Config config,
      StreamApplication app,
      StreamProcessorLifecycleListener listener) {
    Object taskFactory = TaskFactoryUtil.createTaskFactory(config, app, this);
    if (taskFactory instanceof StreamTaskFactory) {
      return new StreamProcessor(
          processorId, config, new HashMap<>(), (StreamTaskFactory) taskFactory, listener);
    } else if (taskFactory instanceof AsyncStreamTaskFactory) {
      return new StreamProcessor(
          processorId, config, new HashMap<>(), (AsyncStreamTaskFactory) taskFactory, listener);
    } else {
      throw new SamzaException(String.format("%s is not a valid task factory",
          taskFactory.getClass().getCanonicalName()));
    }
  }
}
