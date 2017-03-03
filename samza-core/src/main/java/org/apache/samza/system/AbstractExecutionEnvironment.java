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

import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StreamConfig;


public abstract class AbstractExecutionEnvironment implements ExecutionEnvironment {

  private final Config config;
  private final String streamPrefix;

  public AbstractExecutionEnvironment(Config config) {
    if (config == null) {
      throw new NullPointerException();
    }

    this.config = config;
    this.streamPrefix = String.format("%s-%s", config.get(JobConfig.JOB_NAME()), config.get(JobConfig.JOB_ID(), "1"));
  }

  @Override
  public StreamSpec streamFromConfig(String streamId) {
    StreamConfig streamConfig = new StreamConfig(config);

    String system = streamConfig.getSystem(streamId);
    String physicalName = streamConfig.getPhysicalName(streamId, String.format("%s-%s", streamPrefix, streamId));
    Map<String, String> properties = streamConfig.getStreamProperties(streamId);

    return new StreamSpec(streamId, physicalName, system, properties);
  }

  @Override
  public StreamSpec streamFromConfig(String streamId, String physicalName) {
    StreamConfig streamConfig = new StreamConfig(config);

    String system = streamConfig.getSystem(streamId);
    Map<String, String> properties = streamConfig.getStreamProperties(streamId);

    return new StreamSpec(streamId, physicalName, system, properties);
  }

  @Override
  public StreamSpec streamFromConfig(String streamId, String physicalName, String system) {
    StreamConfig streamConfig = new StreamConfig(config);

    Map<String, String> properties = streamConfig.getStreamProperties(streamId);

    return new StreamSpec(streamId, physicalName, system, properties);
  }
}
