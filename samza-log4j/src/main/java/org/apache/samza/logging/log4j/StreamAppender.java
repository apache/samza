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

package org.apache.samza.logging.log4j;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.Log4jSystemConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Util;

/**
 * StreamAppender is a log4j appender that sends logs to the system which is
 * specified by the user in the Samza config. It can send to any system as long
 * as the system is defined appropriately in the config.
 */
public class StreamAppender extends AppenderSkeleton {

  private final String JAVA_OPTS_CONTAINER_NAME = "samza.container.name";
  private final String APPLICATION_MASTER_TAG = "samza-application-master";
  private final String SOURCE = "log4j-log";
  private Config config = null;
  private SystemStream systemStream = null;
  private SystemProducer systemProducer = null;
  private String key = null;
  private String streamName = null;
  private boolean isApplicationMaster = false;
  private Logger log = Logger.getLogger(StreamAppender.class);

  /**
   * used to detect if this thread is called recursively
   */
  private final ThreadLocal<Boolean> recursiveCall = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return false;
    }
  };

  public String getStreamName() {
    return this.streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  @Override
  public void activateOptions() {
    String containerName = System.getProperty(JAVA_OPTS_CONTAINER_NAME);
    isApplicationMaster = containerName.contains(APPLICATION_MASTER_TAG);
    key = containerName; // use the container name as the key for the logs
    config = getConfig();
    SystemFactory systemFactory = null;
    Log4jSystemConfig log4jSystemConfig = new Log4jSystemConfig(config);

    if (streamName == null) {
      streamName = getStreamName(log4jSystemConfig.getJobName(), log4jSystemConfig.getJobId());
    }

    String systemName = log4jSystemConfig.getSystemName();
    String systemFactoryName = log4jSystemConfig.getSystemFactory(systemName);
    if (systemFactoryName != null) {
      systemFactory = Util.<SystemFactory> getObj(systemFactoryName);
    } else {
      throw new SamzaException("Please define log4j system name and factory class");
    }

    systemProducer = systemFactory.getProducer(systemName, config, new MetricsRegistryMap());
    systemStream = new SystemStream(systemName, streamName);
    systemProducer.register(SOURCE);
    systemProducer.start();

    log.info(SOURCE + " has been registered in " + systemName + ". So all the logs will be sent to " + streamName
        + " in " + systemName + ". Logs are partitioned by " + key);
  }

  @Override
  protected void append(LoggingEvent event) {
    if (!recursiveCall.get()) {
      try {
        recursiveCall.set(true);
        OutgoingMessageEnvelope outgoingMessageEnvelope =
            new OutgoingMessageEnvelope(systemStream, key.getBytes("UTF-8"), subAppend(event).getBytes("UTF-8"));
        systemProducer.send(SOURCE, outgoingMessageEnvelope);
      } catch (UnsupportedEncodingException e) {
        throw new SamzaException("can not send the log messages", e);
      } finally {
        recursiveCall.set(false);
      }
    }
  }

  private String subAppend(LoggingEvent event) {
    if (this.layout == null) {
      return event.getRenderedMessage();
    } else {
      return this.layout.format(event).trim();
    }
  }

  @Override
  public void close() {
    if (!this.closed) {
      this.closed = true;
      flushSystemProducer();
      systemProducer.stop();
    }
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }

  /**
   * force the system producer to flush the messages
   */
  public void flushSystemProducer() {
    systemProducer.flush(SOURCE);
  }

  /**
   * get the config for the AM or containers based on the containers' names.
   *
   * @return Config the config of this container
   */
  protected Config getConfig() {
    Config config = null;

    try {
      if (isApplicationMaster) {
        config = SamzaObjectMapper.getObjectMapper().readValue(System.getenv(ShellCommandConfig.ENV_CONFIG()), Config.class);
      } else {
        String url = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
        config = SamzaObjectMapper.getObjectMapper().readValue(Util.read(new URL(url), 30000), JobModel.class).getConfig();
      }
    } catch (IOException e) {
      throw new SamzaException("can not read the config", e);
    }

    return config;
  }

  private String getStreamName(String jobName, String jobId) {
    if (jobName == null) {
      throw new SamzaException("job name is null. Please specify job.name");
    }
    if (jobId == null) {
      jobId = "1";
    }
    String streamName = "__samza_" + jobName + "_" + jobId + "_logs";
    return streamName.replace("-", "_");
  }
}