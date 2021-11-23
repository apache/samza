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
package org.apache.samza.metrics.reporter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistryWithSource;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.MetricsVisitor;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.serializers.Serializer;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MetricsSnapshotReporter is a generic metrics reporter that sends metrics to a stream.
 *
 * jobName // my-samza-job
 * jobId // an id that differentiates multiple executions of the same job
 * taskName // container_567890
 * host // eat1-app128.gird
 * version // 0.0.1
 * blacklist // Regex of metrics to ignore when flushing
 */
public class MetricsSnapshotReporter implements MetricsReporter, Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsSnapshotReporter.class);

  private final SystemProducer producer;
  private final SystemStream out;
  private final Duration reportingInterval;
  private final String jobName;
  private final String jobId;
  private final String containerName;
  private final String version;
  private final String samzaVersion;
  private final String host;
  private final Serializer<MetricsSnapshot> serializer;
  private final Optional<Pattern> blacklist;
  private final Clock clock;

  private final String execEnvironmentContainerId;
  private final ScheduledExecutorService executor;
  private final long resetTime;
  private final List<MetricsRegistryWithSource> registries = new ArrayList<>();
  private final Set<String> blacklistedMetrics = new HashSet<>();

  public MetricsSnapshotReporter(SystemProducer producer, SystemStream out, Duration reportingInterval, String jobName,
      String jobId, String containerName, String version, String samzaVersion, String host,
      Serializer<MetricsSnapshot> serializer, Optional<Pattern> blacklist, Clock clock) {
    this.producer = producer;
    this.out = out;
    this.reportingInterval = reportingInterval;
    this.jobName = jobName;
    this.jobId = jobId;
    this.containerName = containerName;
    this.version = version;
    this.samzaVersion = samzaVersion;
    this.host = host;
    this.serializer = serializer;
    this.blacklist = blacklist;
    this.clock = clock;

    this.execEnvironmentContainerId =
        Optional.ofNullable(System.getenv(ShellCommandConfig.ENV_EXECUTION_ENV_CONTAINER_ID)).orElse("");
    this.executor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("Samza MetricsSnapshotReporter Thread-%d").setDaemon(true).build());
    this.resetTime = this.clock.currentTimeMillis();
    LOG.info(
        "got metrics snapshot reporter properties [job name: {}, job id: {}, containerName: {}, version: {}, samzaVersion: {}, host: {}, reportingInterval {}]",
        jobName, jobId, containerName, version, samzaVersion, host, reportingInterval);
  }

  @Override
  public void start() {
    LOG.info("Starting producer.");
    this.producer.start();
    LOG.info("Starting reporter timer.");
    this.executor.scheduleWithFixedDelay(this, 0, reportingInterval.getSeconds(), TimeUnit.SECONDS);
  }

  @Override
  public void register(String source, ReadableMetricsRegistry registry) {
    this.registries.add(new MetricsRegistryWithSource(source, registry));
    LOG.info("Registering {} with producer.", source);
    this.producer.register(source);
  }

  @Override
  public void stop() {
    // Scheduling an event with 0 delay to ensure flushing of metrics one last time before shutdown
    this.executor.schedule(this, 0, TimeUnit.SECONDS);
    LOG.info("Stopping reporter timer.");
    // Allow the scheduled task above to finish, and block for termination (for max 60 seconds)
    this.executor.shutdown();
    try {
      this.executor.awaitTermination(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new SamzaException(e);
    }
    LOG.info("Stopping producer.");
    this.producer.stop();
    if (!this.executor.isTerminated()) {
      LOG.warn("Unable to shutdown reporter timer.");
    }
  }

  @Override
  public void run() {
    try {
      innerRun();
    } catch (Exception e) {
      // Ignore all exceptions - because subsequent executions of this scheduled task will be suppressed
      // by the executor if the current task throws an unhandled exception.
      LOG.warn("Error while reporting metrics. Will retry in " + reportingInterval + " seconds.", e);
    }
  }

  public void innerRun() {
    LOG.debug("Begin flushing metrics.");
    for (MetricsRegistryWithSource metricsRegistryWithSource : this.registries) {
      String source = metricsRegistryWithSource.getSource();
      ReadableMetricsRegistry registry = metricsRegistryWithSource.getRegistry();
      LOG.debug("Flushing metrics for {}.", source);
      Map<String, Map<String, Object>> metricsMsg = new HashMap<>();

      // metrics
      registry.getGroups().forEach(group -> {
        Map<String, Object> groupMsg = new HashMap<>();
        registry.getGroup(group).forEach((name, metric) -> {
          if (!shouldIgnore(group, name)) {
            metric.visit(new MetricsVisitor() {
              @Override
              public void counter(Counter counter) {
                groupMsg.put(name, counter.getCount());
              }

              @Override
              public <T> void gauge(Gauge<T> gauge) {
                groupMsg.put(name, gauge.getValue());
              }

              @Override
              public void timer(Timer timer) {
                groupMsg.put(name, timer.getSnapshot().getAverage());
              }
            });
          }
        });

        // dont emit empty groups
        if (!groupMsg.isEmpty()) {
          metricsMsg.put(group, groupMsg);
        }
      });

      // publish to Kafka only if the metricsMsg carries any metrics
      if (!metricsMsg.isEmpty()) {
        MetricsHeader header =
            new MetricsHeader(this.jobName, this.jobId, this.containerName, this.execEnvironmentContainerId, source,
                this.version, this.samzaVersion, this.host, this.clock.currentTimeMillis(), this.resetTime);
        Metrics metrics = new Metrics(metricsMsg);
        LOG.debug("Flushing metrics for {} to {} with header and map: header={}, map={}.", source, out,
            header.getAsMap(), metrics.getAsMap());
        MetricsSnapshot metricsSnapshot = new MetricsSnapshot(header, metrics);
        Object maybeSerialized = (this.serializer != null) ? this.serializer.toBytes(metricsSnapshot) : metricsSnapshot;
        try {
          this.producer.send(source, new OutgoingMessageEnvelope(this.out, this.host, null, maybeSerialized));
          // Always flush, since we don't want metrics to get batched up.
          this.producer.flush(source);
        } catch (Exception e) {
          LOG.error(String.format("Exception when flushing metrics for source %s", source), e);
        }
      }
    }
    LOG.debug("Finished flushing metrics.");
  }

  protected boolean shouldIgnore(String group, String metricName) {
    boolean isBlacklisted = this.blacklist.isPresent();
    String fullMetricName = group + "." + metricName;

    if (isBlacklisted && !this.blacklistedMetrics.contains(fullMetricName)) {
      if (this.blacklist.get().matcher(fullMetricName).matches()) {
        this.blacklistedMetrics.add(fullMetricName);
        LOG.debug("Samza diagnostics: blacklisted metric {} because it matched blacklist regex: {}", fullMetricName,
            this.blacklist.get());
      } else {
        isBlacklisted = false;
      }
    }
    return isBlacklisted;
  }
}
