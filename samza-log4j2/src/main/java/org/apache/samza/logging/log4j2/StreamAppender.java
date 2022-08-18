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

package org.apache.samza.logging.log4j2;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.SimpleMessage;
import org.apache.logging.log4j.util.StringMap;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.Log4jSystemConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.logging.LoggingContextHolder;
import org.apache.samza.logging.log4j2.serializers.LoggingEventJsonSerdeFactory;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.MetricsReporterLoader;
import org.apache.samza.util.ReflectionUtil;

@Plugin(name = "Stream", category = "Core", elementType = "appender", printObject = true)
public class StreamAppender extends AbstractAppender {

  private static final String JAVA_OPTS_CONTAINER_NAME = "samza.container.name";
  private static final String SOURCE = "log4j-log";

  // Hidden config for now. Will move to appropriate Config class when ready to.
  private static final String CREATE_STREAM_ENABLED = "task.log4j.create.stream.enabled";

  private static final long DEFAULT_QUEUE_TIMEOUT_S = 2; // Abitrary choice
  private final BlockingQueue<EncodedLogEvent> logQueue = new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE);

  private SystemStream systemStream = null;
  protected SystemProducer systemProducer = null;
  private String key = null;
  private byte[] keyBytes; // Serialize the key once, since we will use it for every event.
  private String containerName = null;
  private int partitionCount = 0;
  private Serde<LogEvent> serde = null;

  private volatile Thread transferThread;
  private Config config = null;
  private String streamName = null;
  private final boolean usingAsyncLogger;
  private final LoggingContextHolder loggingContextHolder;

  /**
   * used to detect if this thread is called recursively
   */
  private final AtomicBoolean recursiveCall = new AtomicBoolean(false);

  protected static final int DEFAULT_QUEUE_SIZE = 100;
  protected volatile boolean systemInitialized = false;
  protected StreamAppenderMetrics metrics;
  protected long queueTimeoutS = DEFAULT_QUEUE_TIMEOUT_S;

  /**
   * Constructor is protected so that this class can be extended.
   */
  protected StreamAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions,
      boolean usingAsyncLogger, String streamName) {
    this(name, filter, layout, ignoreExceptions, usingAsyncLogger, streamName, LoggingContextHolder.INSTANCE);
  }

  /**
   * Constructor is protected so that this class can be extended.
   * @param loggingContextHolder included so that this can be injected for testing purposes in child classes
   */
  protected StreamAppender(String name, Filter filter, Layout<? extends Serializable> layout, boolean ignoreExceptions,
      boolean usingAsyncLogger, String streamName, LoggingContextHolder loggingContextHolder) {
    super(name, filter, layout, ignoreExceptions);
    this.streamName = streamName;
    this.usingAsyncLogger = usingAsyncLogger;
    this.loggingContextHolder = loggingContextHolder;
  }

  @Override
  public void start() {
    super.start();
    containerName = System.getProperty(JAVA_OPTS_CONTAINER_NAME);
    if (containerName == null) {
      throw new SamzaException("Got null container name from system property: " + JAVA_OPTS_CONTAINER_NAME +
          ". This is used as the key for the log appender, so can't proceed.");
    }
    key = containerName; // use the container name as the key for the logs
    try {
      // Serialize the key once, since we will use it for every event.
      keyBytes = key.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new SamzaException(
          String.format("Container name: %s could not be encoded to bytes. %s cannot proceed.", key, getName()), e);
    }
  }

  /**
   * Getter for the StreamName parameter. See also {@link #createAppender(String, Filter, Layout, boolean, boolean, String)} for when this is called.
   * Example: {@literal <param name="StreamName" value="ExampleStreamName"/>}
   * @return The configured stream name.
   */
  public String getStreamName() {
    return this.streamName;
  }

  /**
   * This should only be called after verifying that the {@link LoggingContextHolder} has the config.
   */
  protected Config getConfig() {
    if (config == null) {
      config = this.loggingContextHolder.getConfig();
    }
    return this.config;
  }

  /**
   * Getter for the number of partitions to create on a new StreamAppender stream. See also {@link #createAppender(String, Filter, Layout, boolean, boolean, String)} for when this is called.
   * Example: {@literal <param name="PartitionCount" value="4"/>}
   * This needs to be called after the appender is initialized with the full Samza job config in {@link #setupSystem()}.
   * @return The configured partition count of the StreamAppender stream. If not set, returns {@link JobConfig#getContainerCount()}.
   */
  public int getPartitionCount() {
    if (partitionCount > 0) {
      return partitionCount;
    }
    return new JobConfig(getConfig()).getContainerCount();
  }

  /**
   * Setter for the number of partitions to create on a new StreamAppender stream. See also {@link #createAppender(String, Filter, Layout, boolean, boolean, String)} for when this is called.
   * Example: {@literal <param name="PartitionCount" value="4"/>}
   * @param partitionCount Configurable partition count.
   */
  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }


  @PluginFactory
  public static StreamAppender createAppender(
      @PluginAttribute("name") final String name,
      @PluginElement("Filter") final Filter filter,
      @PluginElement("Layout") Layout layout,
      @PluginAttribute(value = "ignoreExceptions", defaultBoolean = true) final boolean ignoreExceptions,
      @PluginAttribute(value = "usingAsyncLogger", defaultBoolean = false) final boolean usingAsyncLogger,
      @PluginAttribute("streamName") String streamName) {
    return new StreamAppender(name, filter, layout, ignoreExceptions, usingAsyncLogger, streamName);
  }

  @Override
  public void append(LogEvent event) {
    if (!recursiveCall.get()) {
      try {
        recursiveCall.set(true);
        if (!systemInitialized) {
          // configs are needed to set up producer system, so check that before actually initializing
          if (this.loggingContextHolder.getConfig() != null) {
            synchronized (this) {
              if (!systemInitialized) {
                setupSystem();
                systemInitialized = true;
              }
            }
            handleEvent(event);
          } else {
            // skip sending the log to the stream if initialization can't happen yet
            System.out.println("Waiting for config to become available before log can be handled");
          }
        } else {
          handleEvent(event);
        }
      } catch (Exception e) {
        if (metrics != null) { // setupSystem() may not have been invoked yet so metrics can be null here.
          metrics.logMessagesErrors.inc();
        }
        System.err.println(String.format("[%s] Error sending log message:", getName()));
        e.printStackTrace();
      } finally {
        recursiveCall.set(false);
      }
    } else if (metrics != null) { // setupSystem() may not have been invoked yet so metrics can be null here.
      metrics.recursiveCalls.inc();
    }
  }

  /**
   * If async-Logger is enabled, the log-event is sent directly to the systemProducer. Else, the event is serialized
   * and added to a bounded blocking queue, before returning to the "synchronous" caller.
   * @param event the log event to append
   * @throws InterruptedException
   */
  private void handleEvent(LogEvent event) throws InterruptedException {
    if (usingAsyncLogger) {
      sendEventToSystemProducer(encodeLogEvent(event));
      return;
    }

    // Serialize the event before adding to the queue to leverage the caller thread
    // and ensure that the transferThread can keep up.
    if (!logQueue.offer(encodeLogEvent(event), queueTimeoutS, TimeUnit.SECONDS)) {
      // Do NOT retry adding system to the queue. Dropping the event allows us to alleviate the unlikely
      // possibility of a deadlock, which can arise due to a circular dependency between the SystemProducer
      // which is used for StreamAppender and the log, which uses StreamAppender. Any locks held in the callstack
      // of those two code paths can cause a deadlock. Dropping the event allows us to proceed.

      // Scenario:
      // T1: holds L1 and is waiting for L2
      // T2: holds L2 and is waiting to produce to BQ1 which is drained by T3 (SystemProducer) which is waiting for L1

      // This has happened due to locks in Kafka and log4j (see SAMZA-1537), which are both out of our control,
      // so dropping events in the StreamAppender is our best recourse.

      // Drain the queue instead of dropping one message just to reduce the frequency of warn logs above.
      int messagesDropped = logQueue.drainTo(new ArrayList<>()) + 1; // +1 because of the current log event
      System.err.println(String.format("Exceeded timeout %ss while trying to log to %s. Dropping %d log messages.",
          queueTimeoutS,
          systemStream.toString(),
          messagesDropped));

      // Emit a metric which can be monitored to ensure it doesn't happen often.
      metrics.logMessagesDropped.inc(messagesDropped);
    }
    metrics.bufferFillPct.set(Math.round(100f * logQueue.size() / DEFAULT_QUEUE_SIZE));
  }

  protected EncodedLogEvent encodeLogEvent(LogEvent event) {
    return new ByteArrayEncodedLogEvent(serde.toBytes(subLog(event)));
  }

  private Message subAppend(LogEvent event) {
    if (getLayout() == null) {
      return new SimpleMessage(event.getMessage().getFormattedMessage());
    } else {
      Object obj = getLayout().toSerializable(event);
      if (obj instanceof Message) {
        return new SimpleMessage(((Message) obj).getFormattedMessage());
      } else if (obj instanceof LogEvent) {
        return new SimpleMessage(((LogEvent) obj).getMessage().getFormattedMessage());
      } else {
        return new SimpleMessage(obj.toString());
      }
    }
  }

  protected LogEvent subLog(LogEvent event) {
    return Log4jLogEvent.newBuilder()
        .setLevel(event.getLevel())
        .setLoggerName(event.getLoggerName())
        .setLoggerFqcn(event.getLoggerFqcn())
        .setMessage(subAppend(event))
        .setThrown(event.getThrown())
        .setContextData((StringMap) event.getContextData())
        .setContextStack(event.getContextStack())
        .setThreadName(event.getThreadName())
        .setSource(event.getSource())
        .setTimeMillis(event.getTimeMillis())
        .build();
  }

  @Override
  public void stop() {
    System.out.println(String.format("Shutting down the %s...", getName()));
    if (transferThread != null) {
      transferThread.interrupt();
      try {
        transferThread.join();
      } catch (InterruptedException e) {
        System.err.println("Interrupted while waiting for transfer thread to finish." + e);
        Thread.currentThread().interrupt();
      }
    }

    flushSystemProducer();
    if (systemProducer != null) {
      systemProducer.stop();
    }
  }

  /**
   * force the system producer to flush the messages
   */
  private void flushSystemProducer() {
    if (systemProducer != null) {
      systemProducer.flush(SOURCE);
    }
  }

  protected Log4jSystemConfig getLog4jSystemConfig(Config config) {
    return new Log4jSystemConfig(config);
  }

  protected StreamAppenderMetrics getMetrics(MetricsRegistryMap metricsRegistry) {
    return new StreamAppenderMetrics(getName(), metricsRegistry);
  }

  protected void setupStream(SystemFactory systemFactory, String systemName) {
    if (config.getBoolean(CREATE_STREAM_ENABLED, false)) {
      int streamPartitionCount = getPartitionCount();
      System.out.println(String.format("[%s] creating stream ", getName()) + streamName + " with partition count " + streamPartitionCount);
      StreamSpec streamSpec = StreamSpec.createStreamAppenderStreamSpec(streamName, systemName, streamPartitionCount);

      // SystemAdmin only needed for stream creation here.
      SystemAdmin systemAdmin = systemFactory.getAdmin(systemName, config);
      systemAdmin.start();
      systemAdmin.createStream(streamSpec);
      systemAdmin.stop();
    }
  }

  /**
   * This should only be called after verifying that the {@link LoggingContextHolder} has the config.
   */
  protected void setupSystem() {
    config = getConfig();
    Log4jSystemConfig log4jSystemConfig = getLog4jSystemConfig(config);

    if (streamName == null) {
      streamName = getStreamName(log4jSystemConfig.getJobName(), log4jSystemConfig.getJobId());
    }

    // Instantiate metrics
    MetricsRegistryMap metricsRegistry = new MetricsRegistryMap();
    // Take this.getClass().getName() as the name to make it extend-friendly
    metrics = getMetrics(metricsRegistry);
    // Register metrics into metrics reporters so that they are able to be reported to other systems
    Map<String, MetricsReporter>
        metricsReporters = MetricsReporterLoader.getMetricsReporters(new MetricsConfig(config), containerName);
    metricsReporters.values().forEach(reporter -> {
      reporter.register(containerName, metricsRegistry);
      reporter.start();
    });

    String systemName = log4jSystemConfig.getSystemName();
    String systemFactoryName = log4jSystemConfig.getSystemFactory(systemName)
        .orElseThrow(() -> new SamzaException(
            "Could not figure out \"" + systemName + "\" system factory for log4j " + getName() + " to use"));
    SystemFactory systemFactory = ReflectionUtil.getObj(systemFactoryName, SystemFactory.class);

    setSerde(log4jSystemConfig, systemName);

    setupStream(systemFactory, systemName);

    systemProducer = systemFactory.getProducer(systemName, config, metricsRegistry, this.getClass().getSimpleName());
    systemStream = new SystemStream(systemName, streamName);
    systemProducer.register(SOURCE);
    systemProducer.start();

    System.out.println(SOURCE + " has been registered in " + systemName + ". So all the logs will be sent to " + streamName
        + " in " + systemName + ". Logs are partitioned by " + key);

    startTransferThread();
  }

  private void startTransferThread() {
    Runnable transferFromQueueToSystem = () -> {
      while (!Thread.currentThread().isInterrupted()) {
        try {
          sendEventToSystemProducer(logQueue.take());
        } catch (InterruptedException e) {
          // Preserve the interrupted status for the loop condition.
          Thread.currentThread().interrupt();
        } catch (Throwable t) {
          metrics.logMessagesErrors.inc();
          System.err.println("Error sending " + getName() + " event to SystemProducer " + t);
        }
      }
    };

    transferThread = new Thread(transferFromQueueToSystem);
    transferThread.setDaemon(true);
    transferThread.setName("Samza " + getName() + " Producer " + transferThread.getName());
    transferThread.start();
  }

  /**
   * Helper method to send a serialized log-event to the systemProducer, and increment respective methods.
   * @param logQueueEntry the serialized log-event to be sent to the systemProducer
   */
  protected void sendEventToSystemProducer(EncodedLogEvent logQueueEntry) {
    metrics.logMessagesBytesSent.inc(logQueueEntry.getEntryValueSize());
    metrics.logMessagesCountSent.inc();
    systemProducer.send(SOURCE, decorateLogEvent(logQueueEntry));
  }

  /**
   * Helper method to create an OutgoingMessageEnvelope from the serialized log event.
   * @param logQueueEntry message bytes
   * @return OutgoingMessageEnvelope that contains the message bytes along with the system stream
   */
  protected OutgoingMessageEnvelope decorateLogEvent(EncodedLogEvent logQueueEntry) {
    return new OutgoingMessageEnvelope(systemStream, keyBytes, logQueueEntry.getValue());
  }

  protected String getStreamName(String jobName, String jobId) {
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
   */
  protected void setSerde(Log4jSystemConfig log4jSystemConfig, String systemName) {
    String serdeClass = LoggingEventJsonSerdeFactory.class.getCanonicalName();
    String serdeName = log4jSystemConfig.getStreamSerdeName(systemName, streamName);

    if (serdeName != null) {
      serdeClass = log4jSystemConfig.getSerdeClass(serdeName);
    }

    if (serdeClass != null) {
      SerdeFactory<LogEvent> serdeFactory = ReflectionUtil.getObj(serdeClass, SerdeFactory.class);
      serde = serdeFactory.getSerde(systemName, config);
    } else {
      String serdeKey = String.format(SerializerConfig.SERDE_FACTORY_CLASS, serdeName);
      throw new SamzaException("Can not find serializers class for key '" + serdeName + "'. Please specify " +
          serdeKey + " property");
    }
  }

  /**
   * Returns the serde that is being used for the stream appender.
   *
   * @return The Serde&lt;LoggingEvent&gt; that the appender is using.
   */
  public Serde<LogEvent> getSerde() {
    return serde;
  }

  /**
   * A LogQeueEntry is the element inserted into the log queue of the stream appender
   * that holds the log messages before they are sent to the underlying system producer.
   * @param <T> type of object held as the entry
   */
  protected interface EncodedLogEvent<T> {
    /**
     * fetches the size of the log message held within the LogQueueEntry
     * @return size of the log message
     */
    public long getEntryValueSize();

    /**
     * fetches the actual log message held within the LogQueueEntry
     * @return the actual log message
     */
    public T getValue();
  }

  /**
   * LogQueueEntry impl that holds the serialized byte[] of the log message.
   */
  private class ByteArrayEncodedLogEvent implements EncodedLogEvent<byte[]> {
    final byte[] entryValue;
    public ByteArrayEncodedLogEvent(byte[] array) {
      entryValue = array;
    }
    @Override
    public byte[] getValue() {
      return entryValue;
    }

    @Override
    public long getEntryValueSize() {
      return entryValue.length;
    }
  }
}
