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

package org.apache.samza.system.kinesis;

import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;

import org.apache.samza.system.kinesis.consumer.KinesisSystemConsumer;


/**
 * A Kinesis-based implementation of SystemFactory.
 */
public class KinesisSystemFactory implements SystemFactory {
  @Override
  public SystemConsumer getConsumer(String system, Config config, MetricsRegistry registry) {
    KinesisConfig kConfig = new KinesisConfig(config);
    return new KinesisSystemConsumer(system, kConfig, registry);
  }

  @Override
  public SystemProducer getProducer(String system, Config config, MetricsRegistry registry) {
    return null;
  }

  @Override
  public SystemAdmin getAdmin(String system, Config config) {
    validateConfig(system, config);
    KinesisConfig kConfig = new KinesisConfig(config);
    return new KinesisSystemAdmin(system, kConfig);
  }

  protected void validateConfig(String system, Config config) {
    // Kinesis system does not support groupers other than AllSspToSingleTaskGrouper
    JobConfig jobConfig = new JobConfig(config);
    if (!jobConfig.getSystemStreamPartitionGrouperFactory().equals(
        AllSspToSingleTaskGrouperFactory.class.getCanonicalName())) {
      String errMsg = String.format("Incorrect Grouper %s used for KinesisSystemConsumer %s. Please set the %s config"
              + " to %s.", jobConfig.getSystemStreamPartitionGrouperFactory(), system,
          JobConfig.SSP_GROUPER_FACTORY(), AllSspToSingleTaskGrouperFactory.class.getCanonicalName());
      throw new ConfigException(errMsg);
    }

    // Kinesis streams cannot be configured as broadcast streams
    TaskConfigJava taskConfig = new TaskConfigJava(config);
    if (taskConfig.getBroadcastSystemStreams().stream().anyMatch(ss -> system.equals(ss.getSystem()))) {
      throw new ConfigException("Kinesis streams cannot be configured as broadcast streams.");
    }

    // Kinesis streams cannot be configured as bootstrap streams
    KinesisConfig kConfig = new KinesisConfig(config);
    kConfig.getKinesisStreams(system).forEach(stream -> {
        StreamConfig streamConfig = new StreamConfig(kConfig);
        SystemStream ss = new SystemStream(system, stream);
        if (streamConfig.getBootstrapEnabled(ss)) {
          throw new ConfigException("Kinesis streams cannot be configured as bootstrap streams.");
        }
      });
  }
}
