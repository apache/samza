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

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.application.descriptors.ApplicationDescriptor;
import org.apache.samza.application.descriptors.ApplicationDescriptorImpl;
import org.apache.samza.application.descriptors.ApplicationDescriptorUtil;
import org.apache.samza.application.SamzaApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.context.ExternalContext;
import org.apache.samza.execution.LocalJobPlanner;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.task.TaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements the {@link ApplicationRunner} that runs the applications in standalone environment
 */
public class LocalApplicationRunner implements ApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(LocalApplicationRunner.class);

  private final ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc;
  private final LocalJobPlanner planner;
  private final Set<StreamProcessor> processors = ConcurrentHashMap.newKeySet();
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  private final AtomicInteger numProcessorsToStart = new AtomicInteger();
  private final AtomicReference<Throwable> failure = new AtomicReference<>();

  private ApplicationStatus appStatus = ApplicationStatus.New;

  /**
   * Constructors a {@link LocalApplicationRunner} to run the {@code app} with the {@code config}.
   *
   * @param app application to run
   * @param config configuration for the application
   */
  public LocalApplicationRunner(SamzaApplication app, Config config) {
    this.appDesc = ApplicationDescriptorUtil.getAppDescriptor(app, config);
    this.planner = new LocalJobPlanner(appDesc);
  }

  /**
   * Constructor only used in unit test to allow injection of {@link LocalJobPlanner}
   */
  @VisibleForTesting
  LocalApplicationRunner(ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc, LocalJobPlanner planner) {
    this.appDesc = appDesc;
    this.planner = planner;
  }

  @Override
  public void run(ExternalContext externalContext) {
    try {
      List<JobConfig> jobConfigs = planner.prepareJobs();

      // create the StreamProcessors
      if (jobConfigs.isEmpty()) {
        throw new SamzaException("No jobs to run.");
      }
      jobConfigs.forEach(jobConfig -> {
          LOG.debug("Starting job {} StreamProcessor with config {}", jobConfig.getName(), jobConfig);
          StreamProcessor processor = createStreamProcessor(jobConfig, appDesc,
              sp -> new LocalStreamProcessorLifecycleListener(sp, jobConfig), Optional.ofNullable(externalContext));
          processors.add(processor);
        });
      numProcessorsToStart.set(processors.size());

      // start the StreamProcessors
      processors.forEach(StreamProcessor::start);
    } catch (Throwable throwable) {
      appStatus = ApplicationStatus.unsuccessfulFinish(throwable);
      shutdownLatch.countDown();
      throw new SamzaException(String.format("Failed to start application: %s",
          new ApplicationConfig(appDesc.getConfig()).getGlobalAppId()), throwable);
    }
  }

  @Override
  public void kill() {
    processors.forEach(StreamProcessor::stop);
  }

  @Override
  public ApplicationStatus status() {
    return appStatus;
  }

  @Override
  public void waitForFinish() {
    this.waitForFinish(Duration.ofSeconds(0));
  }

  @Override
  public boolean waitForFinish(Duration timeout) {
    long timeoutInMs = timeout.toMillis();
    boolean finished = true;

    try {
      if (timeoutInMs < 1) {
        shutdownLatch.await();
      } else {
        finished = shutdownLatch.await(timeoutInMs, TimeUnit.MILLISECONDS);

        if (!finished) {
          LOG.warn("Timed out waiting for application to finish.");
        }
      }
    } catch (Exception e) {
      LOG.error("Error waiting for application to finish", e);
      throw new SamzaException(e);
    }

    return finished;
  }

  @VisibleForTesting
  protected Set<StreamProcessor> getProcessors() {
    return Collections.unmodifiableSet(processors);
  }

  @VisibleForTesting
  CountDownLatch getShutdownLatch() {
    return shutdownLatch;
  }

  @VisibleForTesting
  StreamProcessor createStreamProcessor(Config config, ApplicationDescriptorImpl<? extends ApplicationDescriptor> appDesc,
      StreamProcessor.StreamProcessorLifecycleListenerFactory listenerFactory,
      Optional<ExternalContext> externalContextOptional) {
    TaskFactory taskFactory = TaskFactoryUtil.getTaskFactory(appDesc);
    Map<String, MetricsReporter> reporters = new HashMap<>();
    String processorId = createProcessorId(new ApplicationConfig(config), getClass().getClassLoader());
    appDesc.getMetricsReporterFactories().forEach((name, factory) ->
        reporters.put(name, factory.getMetricsReporter(name, processorId, config)));
    return new StreamProcessor(processorId, config, reporters, taskFactory, appDesc.getApplicationContainerContextFactory(),
        appDesc.getApplicationTaskContextFactory(), externalContextOptional, listenerFactory, null);
  }

  /**
   * Generates a unique logical identifier for the stream processor using the provided {@param appConfig}.
   * 1. If the processorId is defined in the configuration, then returns the value defined in the configuration.
   * 2. Else if the {@linkplain ProcessorIdGenerator} class is defined the configuration, then uses the {@linkplain ProcessorIdGenerator}
   * to generate the unique processorId.
   * 3. Else throws the {@see ConfigException} back to the caller.
   * @param appConfig the configuration of the samza application.
   * @param classLoader classloader for loading processor id generator
   * @throws ConfigException if neither processor.id nor app.processor-id-generator.class is defined in the configuration.
   * @return the generated processor identifier.
   */
  @VisibleForTesting
  static String createProcessorId(ApplicationConfig appConfig, ClassLoader classLoader) {
    if (StringUtils.isNotBlank(appConfig.getProcessorId())) {
      return appConfig.getProcessorId();
    } else if (StringUtils.isNotBlank(appConfig.getAppProcessorIdGeneratorClass())) {
      ProcessorIdGenerator idGenerator =
          ReflectionUtil.getObj(appConfig.getAppProcessorIdGeneratorClass(), ProcessorIdGenerator.class, classLoader);
      return idGenerator.generateProcessorId(appConfig);
    } else {
      throw new ConfigException(String.format("Expected either %s or %s to be configured", ApplicationConfig.PROCESSOR_ID,
              ApplicationConfig.APP_PROCESSOR_ID_GENERATOR_CLASS));
    }
  }

  /**
   * Defines a specific implementation of {@link ProcessorLifecycleListener} for local {@link StreamProcessor}s.
   */
  private final class LocalStreamProcessorLifecycleListener implements ProcessorLifecycleListener {
    private final StreamProcessor processor;
    private final ProcessorLifecycleListener userDefinedProcessorLifecycleListener;

    LocalStreamProcessorLifecycleListener(StreamProcessor processor, Config jobConfig) {
      this.userDefinedProcessorLifecycleListener = appDesc.getProcessorLifecycleListenerFactory()
          .createInstance(new ProcessorContext() { }, jobConfig);
      this.processor = processor;
    }

    @Override
    public void beforeStart() {
      userDefinedProcessorLifecycleListener.beforeStart();
    }

    @Override
    public void afterStart() {
      if (numProcessorsToStart.decrementAndGet() == 0) {
        appStatus = ApplicationStatus.Running;
      }
      userDefinedProcessorLifecycleListener.afterStart();
    }

    @Override
    public void afterStop() {
      processors.remove(processor);

      // successful shutdown
      handleProcessorShutdown(null);
    }

    @Override
    public void afterFailure(Throwable t) {
      processors.remove(processor);

      // the processor stopped with failure, this is logging the first processor's failure as the cause of
      // the whole application failure
      if (failure.compareAndSet(null, t)) {
        // shutdown the other processors
        processors.forEach(StreamProcessor::stop);
      }

      // handle the current processor's shutdown failure.
      handleProcessorShutdown(t);
    }

    private void handleProcessorShutdown(Throwable error) {
      if (processors.isEmpty()) {
        // all processors are shutdown, setting the application final status
        setApplicationFinalStatus();
      }
      if (error != null) {
        // current processor shutdown with a failure
        userDefinedProcessorLifecycleListener.afterFailure(error);
      } else {
        // current processor shutdown successfully
        userDefinedProcessorLifecycleListener.afterStop();
      }
      if (processors.isEmpty()) {
        // no processor is still running. Notify callers waiting on waitForFinish()
        shutdownLatch.countDown();
      }
    }

    private void setApplicationFinalStatus() {
      if (failure.get() != null) {
        appStatus = ApplicationStatus.unsuccessfulFinish(failure.get());
      } else {
        if (appStatus == ApplicationStatus.Running) {
          appStatus = ApplicationStatus.SuccessfulFinish;
        } else if (appStatus == ApplicationStatus.New) {
          // the processor is shutdown before started
          appStatus = ApplicationStatus.UnsuccessfulFinish;
        }
      }
    }
  }
}
