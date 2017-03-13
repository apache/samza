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
package org.apache.samza.task;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.runtime.ApplicationRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

import scala.runtime.AbstractFunction0;

public class TaskFactoryUtil {
  private static final Logger log = LoggerFactory.getLogger(TaskFactoryUtil.class);

  public static TaskFactory fromTaskClassConfig(Config config, ApplicationRunner runner) {

    String taskClassName;

    // if there is configuration to set the job w/ a specific type of task, instantiate the corresponding task factory
    if (isStreamOperatorTask(config)) {
      taskClassName = StreamOperatorTask.class.getName();
    } else {
      taskClassName = new TaskConfig(config).getTaskClass().getOrElse(
          new AbstractFunction0<String>() {
            @Override
            public String apply() {
              throw new ConfigException("There is no task class defined in the configuration. Failed to create a valid TaskFactory");
            }
          });
    }

    log.info("Got task class name: {}", taskClassName);

    boolean isAsyncTaskClass;
    try {
      isAsyncTaskClass = AsyncStreamTask.class.isAssignableFrom(Class.forName(taskClassName));
    } catch (Throwable t) {
      log.error("Invalid configuration for AsyncStreamTask class: {}. error: {}", taskClassName, t);
      throw new ConfigException(String.format("Invalid configuration for AsyncStreamTask class: %s", taskClassName), t);
    }

    if (isAsyncTaskClass) {
      return new AsyncStreamTaskFactory() {
        @Override
        public AsyncStreamTask createInstance() {
          try {
            return (AsyncStreamTask) Class.forName(taskClassName).newInstance();
          } catch (Throwable t) {
            log.error("Error loading AsyncStreamTask class: {}. error: {}", taskClassName, t);
            throw new SamzaException(String.format("Error loading AsyncStreamTask class: %s", taskClassName), t);
          }
        }
      };
    }

    return new StreamTaskFactory() {
      @Override
      public StreamTask createInstance() {
        try {
          if (taskClassName.equals(StreamOperatorTask.class.getName())) {
            return createStreamOperatorTask(config, runner);
          }
          return (StreamTask) Class.forName(taskClassName).newInstance();
        } catch (Throwable t) {
          log.error("Error loading StreamTask class: {}. error: {}", taskClassName, t);
          throw new SamzaException(String.format("Error loading StreamTask class: %s", taskClassName), t);
        }
      }
    };
  }

  public static TaskFactory finalizeTaskFactory(TaskFactory factory, boolean singleThreadMode, ExecutorService taskThreadPool) {

    validateFactory(factory);

    boolean isAsyncTaskClass = factory instanceof AsyncStreamTaskFactory;
    if (isAsyncTaskClass) {
      log.info("Got an AsyncStreamTask implementation.");
    }

    if (singleThreadMode && isAsyncTaskClass) {
      throw new SamzaException("AsyncStreamTask cannot run on single thread mode.");
    }

    if (!singleThreadMode && !isAsyncTaskClass) {
      log.info("Converting StreamTask to AsyncStreamTaskAdapter when running StreamTask with multiple threads");
      return new AsyncStreamTaskFactory() {
        @Override
        public AsyncStreamTask createInstance() {
          return new AsyncStreamTaskAdapter((StreamTask) factory.createInstance(), taskThreadPool);
        }
      };
    }

    return factory;
  }

  private static void validateFactory(TaskFactory factory) {
    if (factory == null) {
      throw new SamzaException("Either the task class name or the task factory instance is required.");
    }

    if (!(factory instanceof StreamTaskFactory) && !(factory instanceof AsyncStreamTaskFactory)) {
      throw new SamzaException(String.format("TaskFactory must be either StreamTaskFactory or AsyncStreamTaskFactory. %s is not supported",
          factory.getClass()));
    }
  }

  private static StreamTask createStreamOperatorTask(Config config, ApplicationRunner runner) throws Exception {
    Class<?> builderClass = Class.forName(config.get(StreamApplication.APP_CLASS_CONFIG));
    StreamApplication graphBuilder = (StreamApplication) builderClass.newInstance();
    return new StreamOperatorTask(graphBuilder, runner);
  }

  private static boolean isStreamOperatorTask(Config config) {
    if (config.get(StreamApplication.APP_CLASS_CONFIG) != null && !config.get(StreamApplication.APP_CLASS_CONFIG).isEmpty()) {

      TaskConfig taskConfig = new TaskConfig(config);
      if (taskConfig.getTaskClass() != null && !taskConfig.getTaskClass().isEmpty()) {
        throw new ConfigException("High level StreamApplication API cannot be used together with low-level API using task.class.");
      }

      try {
        Class<?> builderClass = Class.forName(config.get(StreamApplication.APP_CLASS_CONFIG));
        return StreamApplication.class.isAssignableFrom(builderClass);
      } catch (Throwable t) {
        log.error("Failed to validate StreamApplication class from the config. {}={}",
            StreamApplication.APP_CLASS_CONFIG, config.get(StreamApplication.APP_CLASS_CONFIG));
        return false;
      }
    }
    return false;
  }
}
