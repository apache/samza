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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.Log4jSystemConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.logging.log4j.serializers.LoggingEventJsonSerdeFactory;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
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

  private static final String JAVA_OPTS_CONTAINER_NAME = "samza.container.name";
  private static final String APPLICATION_MASTER_TAG = "samza-application-master";
  private static final String SOURCE = "log4j-log";
  private Config config = null;
  private SystemStream systemStream = null;
  private SystemProducer systemProducer = null;
  private String key = null;
  private String streamName = null;
  private boolean isApplicationMaster = false;
  private Serde<LoggingEvent> serde = null;
  private Logger log = Logger.getLogger(StreamAppender.class);
  protected static volatile boolean systemInitialized = false;

  /**
   * used to detect if this thread is called recursively
   */
  private final AtomicBoolean recursiveCall = new AtomicBoolean(false);

  public String getStreamName() {
    return this.streamName;
  }

  public void setStreamName(String streamName) {
    this.streamName = streamName;
  }

  @Override
  public void activateOptions() {
    String containerName = System.getProperty(JAVA_OPTS_CONTAINER_NAME);
    if (containerName != null) {
      isApplicationMaster = containerName.contains(APPLICATION_MASTER_TAG);
    } else {
      throw new SamzaException("Got null container name from system property: " + JAVA_OPTS_CONTAINER_NAME +
          ". This is used as the key for the log appender, so can't proceed.");
    }
    key = containerName; // use the container name as the key for the logs
    // StreamAppender has to wait until the JobCoordinator is up when the log is in the AM
    if (isApplicationMaster) {
      systemInitialized = false;
    } else {
      setupSystem();
      systemInitialized = true;
    }
  }

  @Override
  public void append(LoggingEvent event) {
    if (!recursiveCall.get()) {
      try {
        recursiveCall.set(true);
        if (!systemInitialized) {
          if (JobCoordinator.currentJobCoordinator() != null) {
            // JobCoordinator has been instantiated
            setupSystem();
            systemInitialized = true;
          } else {
            log.trace("Waiting for the JobCoordinator to be instantiated...");
          }
        } else {
          OutgoingMessageEnvelope outgoingMessageEnvelope =
              new OutgoingMessageEnvelope(systemStream, key.getBytes("UTF-8"), serde.toBytes(subLog(event)));
          systemProducer.send(SOURCE, outgoingMessageEnvelope);
        }
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

  private LoggingEvent subLog(LoggingEvent event) {
    return new LoggingEvent(event.getFQNOfLoggerClass(), event.getLogger(), event.getTimeStamp(),
        event.getLevel(), subAppend(event), event.getThreadName(), event.getThrowableInformation(),
        event.getNDC(), event.getLocationInformation(), event.getProperties());
  }

  @Override
  public void close() {
    log.info("Shutting down the StreamAppender...");
    if (!this.closed) {
      this.closed = true;
      flushSystemProducer();
      if (systemProducer !=  null) {
        systemProducer.stop();
      }
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
    if (systemProducer != null) {
      systemProducer.flush(SOURCE);
    }
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
        config = JobCoordinator.currentJobCoordinator().jobModel().getConfig();
      } else {
        String url = System.getenv(ShellCommandConfig.ENV_COORDINATOR_URL());
        config = SamzaObjectMapper.getObjectMapper()
            .readValue(Util.read(new URL(url), 30000), JobModel.class)
            .getConfig();
      }
    } catch (IOException e) {
      throw new SamzaException("can not read the config", e);
    }

    return config;
  }

  protected void setupSystem() {
    config = getConfig();
    SystemFactory systemFactory = null;
    Log4jSystemConfig log4jSystemConfig = new Log4jSystemConfig(config);

    if (streamName == null) {
      streamName = getStreamName(log4jSystemConfig.getJobName(), log4jSystemConfig.getJobId());
    }

    String systemName = log4jSystemConfig.getSystemName();
    String systemFactoryName = log4jSystemConfig.getSystemFactory(systemName);
    if (systemFactoryName != null) {
      systemFactory = Util.<SystemFactory>getObj(systemFactoryName);
    } else {
      throw new SamzaException("Could not figure out \"" + systemName + "\" system factory for log4j StreamAppender to use");
    }

    setSerde(log4jSystemConfig, systemName, streamName);

    systemProducer = systemFactory.getProducer(systemName, config, new MetricsRegistryMap());
    systemStream = new SystemStream(systemName, streamName);
    systemProducer.register(SOURCE);
    systemProducer.start();

    log.info(SOURCE + " has been registered in " + systemName + ". So all the logs will be sent to " + streamName
        + " in " + systemName + ". Logs are partitioned by " + key);
  }

  protected static String getStreamName(String jobName, String jobId) {
    if (jobName == null) {
      throw new SamzaException("job name is null. Please specify job.name");
    }
    if (jobId == null) {
      jobId = "1";
    }
    String streamName = "__samza_" + jobName + "_" + jobId + "_logs";
    return streamName.replace("-", "_");
  }

  /**
   * set the serde for this appender. It looks for the stream serde first, then system serde.
   * If still can not get the serde, throws exceptions. 
   *
   * @param log4jSystemConfig log4jSystemConfig for this appender
   * @param systemName name of the system
   * @param streamName name of the stream
   */
  private void setSerde(Log4jSystemConfig log4jSystemConfig, String systemName, String streamName) {
    String serdeClass = LoggingEventJsonSerdeFactory.class.getCanonicalName();
    String serdeName = log4jSystemConfig.getStreamSerdeName(systemName, streamName);

    if (serdeName != null) {
      serdeClass = log4jSystemConfig.getSerdeClass(serdeName);
    }

    if (serdeClass != null) {
      SerdeFactory<LoggingEvent> serdeFactory = Util.<SerdeFactory<LoggingEvent>>getObj(serdeClass);
      serde = serdeFactory.getSerde(systemName, config);
    } else {
      String serdeKey = String.format(SerializerConfig.SERDE(), serdeName);
      throw new SamzaException("Can not find serializers class for key '" + serdeName + "'. Please specify " +
          serdeKey + " property");
    }
  }

  /**
   * Returns the serde that is being used for the stream appender.
   * 
   * @return The Serde&lt;LoggingEvent&gt; that the appender is using.
   */
  public Serde<LoggingEvent> getSerde() {
    return serde;
  }
}