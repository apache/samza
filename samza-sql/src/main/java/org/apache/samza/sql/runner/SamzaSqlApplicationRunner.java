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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.runtime.AbstractApplicationRunner;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.runtime.RemoteApplicationRunner;
import org.apache.samza.sql.interfaces.SourceResolver;
import org.apache.samza.sql.interfaces.SqlSystemSourceConfig;
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
public class SamzaSqlApplicationRunner extends AbstractApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaSqlApplicationRunner.class);

  private final Config sqlConfig;
  private final ApplicationRunner appRunner;
  private final Boolean localRunner;

  public static final String RUNNER_CONFIG = "app.runner.class";
  public static final String CFG_FMT_SAMZA_STREAM_SYSTEM = "streams.%s.samza.system";

  public SamzaSqlApplicationRunner(Config config) {
    this(false, config);
  }

  public SamzaSqlApplicationRunner(Boolean localRunner, Config config) {
    super(config);
    this.localRunner = localRunner;
    sqlConfig = computeSamzaConfigs(localRunner, config);
    appRunner = ApplicationRunner.fromConfig(sqlConfig);
  }

  public static Config computeSamzaConfigs(Boolean localRunner, Config config) {
    Map<String, String> newConfig = new HashMap<>();

    SourceResolver sourceResolver = SamzaSqlApplicationConfig.createSourceResolver(config);
    // Parse the sql and find the input stream streams
    List<String> sqlStmts = SamzaSqlApplicationConfig.fetchSqlFromConfig(config);

    // This is needed because the SQL file may not be available in all the node managers.
    String sqlJson = SamzaSqlApplicationConfig.serializeSqlStmts(sqlStmts);
    newConfig.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, sqlJson);

    List<SamzaSqlQueryParser.QueryInfo> queryInfo = SamzaSqlApplicationConfig.fetchQueryInfo(sqlStmts);
    for (SamzaSqlQueryParser.QueryInfo query : queryInfo) {
      // Populate stream to system mapping config for input and output system streams
      for (String inputSource : query.getInputSources()) {
        SqlSystemSourceConfig inputSystemStreamConfig = sourceResolver.fetchSourceInfo(inputSource);
        newConfig.put(String.format(CFG_FMT_SAMZA_STREAM_SYSTEM, inputSystemStreamConfig.getStreamName()),
            inputSystemStreamConfig.getSystemName());
        newConfig.putAll(inputSystemStreamConfig.getConfig());
      }

      SqlSystemSourceConfig outputSystemStreamConfig = sourceResolver.fetchSourceInfo(query.getOutputSource());
      newConfig.put(String.format(CFG_FMT_SAMZA_STREAM_SYSTEM, outputSystemStreamConfig.getStreamName()),
          outputSystemStreamConfig.getSystemName());
      newConfig.putAll(outputSystemStreamConfig.getConfig());
    }

    if (localRunner) {
      newConfig.put(RUNNER_CONFIG, LocalApplicationRunner.class.getName());
    } else {
      newConfig.put(RUNNER_CONFIG, RemoteApplicationRunner.class.getName());
    }

    newConfig.putAll(config);

    LOG.info("New Samza configs: " + newConfig);
    return new MapConfig(newConfig);
  }

  public void runAndWaitForFinish() {
    Validate.isTrue(localRunner, "This method can be called only in standalone mode.");
    SamzaSqlApplication app = new SamzaSqlApplication();
    run(app);
    ((LocalApplicationRunner) appRunner).waitForFinish();
  }

  @Override
  public void runTask() {
    appRunner.runTask();
  }

  @Override
  public void run(StreamApplication streamApp) {
    super.run(streamApp);
    Validate.isInstanceOf(SamzaSqlApplication.class, streamApp);
    appRunner.run(streamApp);
  }

  @Override
  public void kill(StreamApplication streamApp) {
    appRunner.kill(streamApp);
    super.kill(streamApp);
  }

  @Override
  public ApplicationStatus status(StreamApplication streamApp) {
    return appRunner.status(streamApp);
  }
}
