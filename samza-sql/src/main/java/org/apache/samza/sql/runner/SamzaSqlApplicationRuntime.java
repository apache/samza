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

package org.apache.samza.sql.runner;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.apache.samza.application.ApplicationSpec;
import org.apache.samza.application.internal.StreamAppSpecImpl;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.runtime.ApplicationRuntime;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.runtime.RemoteApplicationRunner;
import org.apache.samza.runtime.internal.ApplicationRunner;
import org.apache.samza.runtime.internal.ApplicationRunners;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Application runner implementation for SamzaSqlApplication.
 * SamzaSqlApplication needs SamzaSqlConfigRewriter to infer some of the configs from SQL statements.
 * Since Samza's config rewriting capability is available only in the RemoteApplicationRunner.
 * This runner invokes the SamzaSqlConfig re-writer if it is invoked on a standalone mode (i.e. localRunner == true)
 * otherwise directly calls the RemoteApplicationRunner which automatically performs the config rewriting .
 */
public class SamzaSqlApplicationRuntime implements ApplicationRuntime {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaSqlApplicationRuntime.class);

  private final Config sqlConfig;
  private final ApplicationSpec appSpec;
  private final ApplicationRunner runner;
  private final Boolean localRunner;

  public static final String RUNNER_CONFIG = "app.runner.class";
  public static final String CFG_FMT_SAMZA_STREAM_SYSTEM = "streams.%s.samza.system";

  public SamzaSqlApplicationRuntime(Boolean localRunner, Config config) {
    this.localRunner = localRunner;
    sqlConfig = computeSamzaConfigs(localRunner, config);
    appSpec = new StreamAppSpecImpl(new SamzaSqlApplication(), sqlConfig);
    runner = ApplicationRunners.fromConfig(sqlConfig);
  }

  public static Config computeSamzaConfigs(Boolean localRunner, Config config) {
    Map<String, String> newConfig = new HashMap<>();

    SqlIOResolver ioResolver = SamzaSqlApplicationConfig.createIOResolver(config);
    // Parse the sql and find the input stream streams
    List<String> sqlStmts = SamzaSqlApplicationConfig.fetchSqlFromConfig(config);

    // This is needed because the SQL file may not be available in all the node managers.
    String sqlJson = SamzaSqlApplicationConfig.serializeSqlStmts(sqlStmts);
    newConfig.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, sqlJson);

    List<SamzaSqlQueryParser.QueryInfo> queryInfo = SamzaSqlApplicationConfig.fetchQueryInfo(sqlStmts);
    for (SamzaSqlQueryParser.QueryInfo query : queryInfo) {
      // Populate stream to system mapping config for input and output system streams
      for (String inputSource : query.getSources()) {
        SqlIOConfig inputSystemStreamConfig = ioResolver.fetchSourceInfo(inputSource);
        newConfig.put(String.format(CFG_FMT_SAMZA_STREAM_SYSTEM, inputSystemStreamConfig.getStreamName()),
            inputSystemStreamConfig.getSystemName());
        newConfig.putAll(inputSystemStreamConfig.getConfig());
      }

      SqlIOConfig outputSystemStreamConfig = ioResolver.fetchSinkInfo(query.getSink());
      newConfig.put(String.format(CFG_FMT_SAMZA_STREAM_SYSTEM, outputSystemStreamConfig.getStreamName()),
          outputSystemStreamConfig.getSystemName());
      newConfig.putAll(outputSystemStreamConfig.getConfig());
    }

    newConfig.putAll(config);

    if (localRunner) {
      newConfig.put(RUNNER_CONFIG, LocalApplicationRunner.class.getName());
    } else {
      newConfig.put(RUNNER_CONFIG, RemoteApplicationRunner.class.getName());
    }

    LOG.info("New Samza configs: " + newConfig);
    return new MapConfig(newConfig);
  }

  public void runAndWaitForFinish() {
    Validate.isTrue(localRunner, "This method can be called only in standalone mode.");
    run();
    waitForFinish();
  }

  @Override
  public void run() {
    runner.run(appSpec);
  }

  @Override
  public void kill() {
    runner.kill(appSpec);
  }

  @Override
  public ApplicationStatus status() {
    return runner.status(appSpec);
  }

  @Override
  public void waitForFinish() {
    runner.waitForFinish(appSpec);
  }

  @Override
  public boolean waitForFinish(Duration timeout) {
    return runner.waitForFinish(appSpec, timeout);
  }

  @Override
  public void addMetricsReporters(Map<String, MetricsReporter> metricsReporters) {
    runner.addMetricsReporters(metricsReporters);
  }
}
