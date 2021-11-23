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
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.config.StreamConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.MetricsReporterFactory;
import org.apache.samza.serializers.MetricsSnapshotSerdeV2;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.util.StreamUtil;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricsSnapshotReporterFactory implements MetricsReporterFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsSnapshotReporterFactory.class);

  @Override
  public MetricsReporter getMetricsReporter(String reporterName, String containerName, Config config) {
    LOG.info("Creating new metrics snapshot reporter.");
    MetricsRegistryMap registry = new MetricsRegistryMap();

    SystemStream systemStream = getSystemStream(reporterName, config);
    SystemProducer producer = getProducer(reporterName, config, registry);
    Duration reportingInterval = Duration.ofSeconds(getReportingInterval(reporterName, config));
    String jobName = getJobName(config);
    String jobId = getJobId(config);
    Serde<MetricsSnapshot> serde = getSerde(reporterName, config);
    Optional<Pattern> blacklist = getBlacklist(reporterName, config);

    MetricsSnapshotReporter reporter =
        new MetricsSnapshotReporter(producer, systemStream, reportingInterval, jobName, jobId, containerName,
            Util.getTaskClassVersion(config), Util.getSamzaVersion(), Util.getLocalHost().getHostName(), serde,
            blacklist, SystemClock.instance());
    reporter.register(this.getClass().getSimpleName(), registry);
    return reporter;
  }

  protected SystemProducer getProducer(String reporterName, Config config, MetricsRegistryMap registry) {
    SystemConfig systemConfig = new SystemConfig(config);
    String systemName = getSystemStream(reporterName, config).getSystem();
    String systemFactoryClassName = systemConfig.getSystemFactory(systemName)
        .orElseThrow(() -> new SamzaException(
            String.format("Trying to fetch system factory for system %s, which isn't defined in config.", systemName)));
    SystemFactory systemFactory = ReflectionUtil.getObj(systemFactoryClassName, SystemFactory.class);
    LOG.info("Got system factory {}.", systemFactory);
    SystemProducer producer = systemFactory.getProducer(systemName, config, registry);
    LOG.info("Got producer {}.", producer);
    return producer;
  }

  protected SystemStream getSystemStream(String reporterName, Config config) {
    MetricsConfig metricsConfig = new MetricsConfig(config);
    String metricsSystemStreamName = metricsConfig.getMetricsSnapshotReporterStream(reporterName)
        .orElseThrow(() -> new SamzaException("No metrics stream defined in config."));
    SystemStream systemStream = StreamUtil.getSystemStreamFromNames(metricsSystemStreamName);
    LOG.info("Got system stream {}.", systemStream);
    return systemStream;
  }

  protected Serde<MetricsSnapshot> getSerde(String reporterName, Config config) {
    StreamConfig streamConfig = new StreamConfig(config);
    SystemConfig systemConfig = new SystemConfig(config);
    SystemStream systemStream = getSystemStream(reporterName, config);

    Optional<String> streamSerdeName = streamConfig.getStreamMsgSerde(systemStream);
    Optional<String> systemSerdeName = systemConfig.getSystemMsgSerde(systemStream.getSystem());
    String serdeName = streamSerdeName.orElse(systemSerdeName.orElse(null));
    SerializerConfig serializerConfig = new SerializerConfig(config);
    Serde<MetricsSnapshot> serde;
    if (serdeName != null) {
      Optional<String> serdeFactoryClass = serializerConfig.getSerdeFactoryClass(serdeName);
      if (serdeFactoryClass.isPresent()) {
        SerdeFactory<MetricsSnapshot> serdeFactory = ReflectionUtil.getObj(serdeFactoryClass.get(), SerdeFactory.class);
        serde = serdeFactory.getSerde(serdeName, config);
      } else {
        serde = null;
      }
    } else {
      serde = new MetricsSnapshotSerdeV2();
    }
    LOG.info("Got serde {}.", serde);
    return serde;
  }

  protected Optional<Pattern> getBlacklist(String reporterName, Config config) {
    MetricsConfig metricsConfig = new MetricsConfig(config);
    Optional<String> blacklist = metricsConfig.getMetricsSnapshotReporterBlacklist(reporterName);
    LOG.info("Got blacklist as: {}", blacklist);
    return blacklist.map(Pattern::compile);
  }

  protected int getReportingInterval(String reporterName, Config config) {
    MetricsConfig metricsConfig = new MetricsConfig(config);
    int reportingInterval = metricsConfig.getMetricsSnapshotReporterInterval(reporterName);
    LOG.info("Got reporting interval: {}", reportingInterval);
    return reportingInterval;
  }

  protected String getJobId(Config config) {
    JobConfig jobConfig = new JobConfig(config);
    return jobConfig.getJobId();
  }

  protected String getJobName(Config config) {
    JobConfig jobConfig = new JobConfig(config);
    return jobConfig.getName().orElseThrow(() -> new SamzaException("Job name must be defined in config."));
  }
}
