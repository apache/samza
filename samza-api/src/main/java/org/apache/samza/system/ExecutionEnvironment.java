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
package org.apache.samza.system;

import java.lang.reflect.Constructor;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.ConfigException;
import org.apache.samza.operators.StreamGraphBuilder;
import org.apache.samza.config.Config;


/**
 * Interface to be implemented by physical execution engine to deploy the config and jobs to run the {@link org.apache.samza.operators.StreamGraph}
 */
@InterfaceStability.Unstable
public interface ExecutionEnvironment {

  String ENVIRONMENT_CONFIG = "job.execution.environment.class";
  String DEFAULT_ENVIRONMENT_CLASS = "org.apache.samza.system.StandaloneExecutionEnvironment";

  /**
   * Static method to load the local standalone environment
   *
   * @param config  configuration passed in to initialize the Samza standalone process
   * @return  the standalone {@link ExecutionEnvironment} to run the user-defined stream applications
   */
  static ExecutionEnvironment getLocalEnvironment(Config config) {
    return null;
  }

  /**
   * Static method to load the non-standalone environment.
   *
   * @param config  configuration passed in to initialize the Samza processes
   * @return  the configure-driven {@link ExecutionEnvironment} to run the user-defined stream applications
   */
  static ExecutionEnvironment fromConfig(Config config) {
    try {
      Class<?> environmentClass = Class.forName(config.get(ENVIRONMENT_CONFIG, DEFAULT_ENVIRONMENT_CLASS));
      if (ExecutionEnvironment.class.isAssignableFrom(environmentClass)) {
        Constructor<?> constructor = environmentClass.getConstructor(Config.class); // *sigh*
        return (ExecutionEnvironment) constructor.newInstance(config);
      }
    } catch (Exception e) {
      throw new ConfigException(String.format("Problem in loading ExecutionEnvironment class %s", config.get(ENVIRONMENT_CONFIG)), e);
    }
    throw new ConfigException(String.format(
        "Class %s does not implement interface ExecutionEnvironment properly",
        config.get(ENVIRONMENT_CONFIG)));
  }

  /**
   * Method to be invoked to deploy and run the actual Samza jobs to execute {@link org.apache.samza.operators.StreamGraph}
   *
   * @param graphBuilder  the user-defined {@link StreamGraphBuilder} object
   * @param config  the {@link Config} object for this job
   */
  void run(StreamGraphBuilder graphBuilder, Config config);

  /**
   * Constructs a {@link StreamSpec} from the configuration for the specified streamId.
   *
   * The stream configurations are read from the following properties in the config:
   * {@code streams.{$streamId}.*}
   * <br>
   * All properties matching this pattern are assumed to be system-specific with two exceptions. The following two
   * properties are Samza properties which are used to bind the stream to a system and a physical resource on that system.
   *
   * <ul>
   *   <li>samza.system -         The name of the System on which this stream will be used. If this property isn't defined
   *                              the stream will be associated with the System defined in {@code job.default.system}</li>
   *   <li>samza.physical.name -  The system-specific name for this stream. It could be a file URN, topic name, or other identifer.
   *                              If this property isn't defined the physical.name will be set to the streamId</li>
   * </ul>
   *
   * @param streamId  The logical identifier for the stream in Samza.
   * @return          The {@link StreamSpec} instance.
   */
  StreamSpec streamFromConfig(String streamId);
}
