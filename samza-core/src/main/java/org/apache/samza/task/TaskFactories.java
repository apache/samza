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

import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.operators.StreamGraphBuilder;
import org.apache.samza.config.TaskConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.util.ScalaToJavaUtils.defaultValue;

public class TaskFactories {
  private static final Logger log = LoggerFactory.getLogger(TaskFactories.class);

  public static Object fromTaskClassConfig(Config config) throws ClassNotFoundException {

    String taskClassName;

    // if there is configuration to set the job w/ a specific type of task, instantiate the corresponding task factory
    if (isStreamOperatorTask(config)) {
      taskClassName = StreamOperatorTask.class.getName();
    } else {
      taskClassName = new TaskConfig(config).getTaskClass().getOrElse(defaultValue(null));
    }

    if (taskClassName == null) {
      throw new ConfigException("There is no task class defined in the configuration. Failed to create a valid TaskFactory");
    }
    log.info("Got task class name: {}", taskClassName);

    boolean isAsyncTaskClass = AsyncStreamTask.class.isAssignableFrom(Class.forName(taskClassName));
    if (isAsyncTaskClass) {
      return new AsyncStreamTaskFactory() {
        @Override
        public AsyncStreamTask createInstance() {
          try {
            return (AsyncStreamTask) Class.forName(taskClassName).newInstance();
          } catch (Exception e) {
            log.error("Error loading AsyncStreamTask class: {}. error: {}", taskClassName, e);
            throw new RuntimeException(e);
          }
        }
      };
    }

    return new StreamTaskFactory() {
      @Override
      public StreamTask createInstance() {
        try {
          return taskClassName == StreamOperatorTask.class.getName() ? createStreamOperatorTask(config) :
              (StreamTask) Class.forName(taskClassName).newInstance();
        } catch (Exception e) {
          log.error("Error loading StreamTask class: {}. error: {}", taskClassName, e);
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static StreamTask createStreamOperatorTask(Config config) throws Exception {
    StreamGraphBuilder graphBuilder = (StreamGraphBuilder) Class.forName(config.get(StreamGraphBuilder.BUILDER_CLASS_CONFIG)).newInstance();
    return new StreamOperatorTask(graphBuilder);
  }

  private static boolean isStreamOperatorTask(Config config) {
    try {
      if (config.get(StreamGraphBuilder.BUILDER_CLASS_CONFIG) != null && config.get(StreamGraphBuilder.BUILDER_CLASS_CONFIG) != "") {
        return StreamGraphBuilder.class.isAssignableFrom(Class.forName(config.get(StreamGraphBuilder.BUILDER_CLASS_CONFIG)));
      }
      return false;
    } catch (Exception e) {
      log.error("Failed to validate StreamGraphBuilder class from the config. {}={}",
          StreamGraphBuilder.BUILDER_CLASS_CONFIG, config.get(StreamGraphBuilder.BUILDER_CLASS_CONFIG));
      return false;
    }
  }
}
