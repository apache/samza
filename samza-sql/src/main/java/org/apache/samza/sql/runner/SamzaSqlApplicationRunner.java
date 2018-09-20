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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.runtime.RemoteApplicationRunner;
import org.apache.samza.sql.dsl.SamzaSqlDslConverter;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Application runner implementation for SamzaSqlApplication.
 * SamzaSqlApplication needs SamzaSqlConfigRewriter to infer some of the configs from SQL statements.
 * Since Samza's config rewriting capability is available only in the RemoteApplicationRunner.
 * This runner invokes the SamzaSqlConfig re-writer if it is invoked on a standalone mode (i.e. localRunner == true)
 * otherwise directly calls the RemoteApplicationRunner which automatically performs the config rewriting .
 */
public class SamzaSqlApplicationRunner implements ApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaSqlApplicationRunner.class);

  private final ApplicationRunner runner;

  public static final String RUNNER_CONFIG = "app.runner.class";
  public static final String CFG_FMT_SAMZA_STREAM_SYSTEM = "streams.%s.samza.system";

  public SamzaSqlApplicationRunner(Boolean localRunner, Config config) {
    this.runner = ApplicationRunners.getApplicationRunner(new SamzaSqlApplication(),
        computeSamzaConfigs(localRunner, config));
  }

  public static Config computeSamzaConfigs(Boolean localRunner, Config config) {
    Map<String, String> newConfig = new HashMap<>();

    // TODO: Introduce an API to return a dsl string containing one or more sql statements
    List<String> dslStmts = SamzaSqlDslConverter.fetchSqlFromConfig(config);

    // This is needed because the SQL file may not be available in all the node managers.
    String sqlJson = SamzaSqlApplicationConfig.serializeSqlStmts(dslStmts);
    newConfig.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, sqlJson);

    Set<String> inputSystemStreams = new HashSet<>();
    Set<String> outputSystemStreams = new HashSet<>();

    SamzaSqlApplicationConfig.populateSystemStreamsAndGetRelRoots(dslStmts, config,
        inputSystemStreams, outputSystemStreams);

    SqlIOResolver ioResolver = SamzaSqlApplicationConfig.createIOResolver(config);

    // Populate stream to system mapping config for input and output system streams
    for (String source : inputSystemStreams) {
      SqlIOConfig inputSystemStreamConfig = ioResolver.fetchSourceInfo(source);
      newConfig.put(String.format(CFG_FMT_SAMZA_STREAM_SYSTEM, inputSystemStreamConfig.getStreamName()),
          inputSystemStreamConfig.getSystemName());
      newConfig.putAll(inputSystemStreamConfig.getConfig());
    }

    for (String sink : outputSystemStreams) {
      SqlIOConfig outputSystemStreamConfig = ioResolver.fetchSinkInfo(sink);
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
    Validate.isTrue(runner instanceof LocalApplicationRunner, "This method can be called only in standalone mode.");
    run();
    waitForFinish();
  }

  @Override
  public void run() {
    runner.run();
  }

  @Override
  public void kill() {
    runner.kill();
  }

  @Override
  public ApplicationStatus status() {
    return runner.status();
  }

  @Override
  public void waitForFinish() {
    runner.waitForFinish();
  }

  @Override
  public boolean waitForFinish(Duration timeout) {
    return runner.waitForFinish(timeout);
  }

}
