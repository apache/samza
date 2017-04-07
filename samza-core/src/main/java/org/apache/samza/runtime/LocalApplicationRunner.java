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

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.CoordinationServiceFactory;
import org.apache.samza.coordinator.CoordinationUtils;
import org.apache.samza.coordinator.Latch;
import org.apache.samza.execution.ExecutionPlan;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifeCycleAware;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.task.AsyncStreamTaskFactory;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskFactoryUtil;
import org.apache.samza.util.*;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements the {@link ApplicationRunner} that runs the applications in standalone environment
 */
public class LocalApplicationRunner extends AbstractApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(LocalApplicationRunner.class);
  private static final String LATCH_INIT = "init";
  private static final long LATCH_TIMEOUT = 10; // 10 min timeout

  // TODO: these should be replaced by processorId generator
  private static final String PROCESSOR_ID = "processor.id";
  private int processorId;

  private final CoordinationUtils coordination;
  private final CountDownLatch latch = new CountDownLatch(1);
  private final ConcurrentHashSet<StreamProcessor> processors = new ConcurrentHashSet<>();
  private final AtomicReference<Throwable> throwable = new AtomicReference<>();

  ApplicationStatus appStatus = ApplicationStatus.New;

  final class LocalProcessorListener implements StreamProcessorLifeCycleAware {
    StreamProcessor processor;

    LocalProcessorListener(StreamProcessor processor) {
      this.processor = processor;
    }

    @Override
    public void onContainerStart() {
    }

    @Override
    public void onContainerShutdown() {
      processors.remove(processor);
      if (processors.isEmpty()) {
        latch.countDown();
      }
    }

    @Override
    public void onContainerFailure(Throwable t) {
      throwable.compareAndSet(null, t);
    }
  }

  public LocalApplicationRunner(Config config) throws Exception{
    super(config);
    processorId = Integer.valueOf(config.get(PROCESSOR_ID));
    coordination = getCoordinationUtils();
  }

  @Override
  public void run(StreamApplication app) {
    try {
      // 1. initialize and plan
      ExecutionPlan plan = getExecutionPlan(app);

      // 2. create the necessary streams
      List<StreamSpec> intStreams = plan.getIntermediateStreams();
      if (!intStreams.isEmpty()) {
        if (coordination != null) {
          Latch initLatch = coordination.getLatch(1, LATCH_INIT);
          coordination.getLeaderElector().tryBecomeLeader(() -> {
            getStreamManager().createStreams(intStreams);
            initLatch.countDown();
          });
          initLatch.await(LATCH_TIMEOUT, TimeUnit.MINUTES);
        } else {
          // each application process will try creating the streams, which
          // requires stream creation to be idempotent
          getStreamManager().createStreams(intStreams);
        }
      }

      // 3. start the StreamProcessors
      plan.getJobConfigs().forEach(jobConfig -> {
        log.info("Starting job {} StreamProcessor with config {}", jobConfig.getName(), jobConfig);

        Object taskFactory = TaskFactoryUtil.createTaskFactory(config, app, this);
        StreamProcessor processor = createStreamProcessor(jobConfig, taskFactory);
        processor.addLifeCycleAware(new LocalProcessorListener(processor));
        processors.add(processor);
        processor.start();
      });
      appStatus = ApplicationStatus.Running;

      // 4. block until the processors are done or there is a failure
      latch.await();
      if (throwable.get() != null) {
        appStatus = ApplicationStatus.UnsuccessfulFinish;
        throw throwable.get();
      } else {
        appStatus = ApplicationStatus.SuccessfulFinish;
      }

    } catch (Throwable t) {
      throw new SamzaException("Failed to run application", t);
    } finally {
      coordination.reset();
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

  private CoordinationUtils getCoordinationUtils() throws Exception {
    ApplicationConfig appConfig = new ApplicationConfig(config);
    String clazz = appConfig.getCoordinationServiceFactoryClass();
    if (clazz != null) {
      CoordinationServiceFactory factory = ClassLoaderHelper.fromClassName(clazz);
      String groupId = String.format("app-%s-%s", config.get(JobConfig.JOB_NAME()), config.get(JobConfig.JOB_ID(), "1"));
      return factory.getCoordinationService(groupId, String.valueOf(processorId), config);
    }
    else {
      return null;
    }
  }

  private StreamProcessor createStreamProcessor(Config config, Object taskFactory) {
    if (taskFactory instanceof StreamTaskFactory) {
      return new StreamProcessor(processorId, config, new HashMap<>(), (StreamTaskFactory) taskFactory);
    } else if (taskFactory instanceof AsyncStreamTaskFactory) {
      return new StreamProcessor(processorId, config, new HashMap<>(), (AsyncStreamTaskFactory) taskFactory);
    } else {
      throw new SamzaException(String.format("%s is not a valid task factory",
          taskFactory.getClass().getCanonicalName().toString()));
    }
  }


}
