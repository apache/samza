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
package org.apache.samza.lineage;

import java.util.Optional;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.LineageConfig;
import org.apache.samza.util.ReflectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The LineageEmitter class helps generate and emit job lineage data to configured sink stream.
 */
public final class LineageEmitter {

  public static final Logger LOGGER = LoggerFactory.getLogger(LineageEmitter.class);

  /**
   * Emit the job lineage information to specified sink stream.
   * @param config Samza job config
   */
  public static void emit(Config config) {
    LineageConfig lineageConfig = new LineageConfig(config);
    Optional<String> lineageFactoryClassName = lineageConfig.getLineageFactoryClassName();
    Optional<String> lineageReporterFactoryClassName = lineageConfig.getLineageReporterFactoryClassName();

    if (!lineageFactoryClassName.isPresent() && !lineageReporterFactoryClassName.isPresent()) {
      return;
    }
    if (!lineageFactoryClassName.isPresent()) {
      throw new ConfigException(String.format("Missing the lineage config: %s", LineageConfig.LINEAGE_FACTORY));
    }
    if (!lineageReporterFactoryClassName.isPresent()) {
      throw new ConfigException(String.format("Missing the lineage config: %s", LineageConfig.LINEAGE_REPORTER_FACTORY));
    }

    LineageFactory lineageFactory = ReflectionUtil.getObj(lineageFactoryClassName.get(), LineageFactory.class);
    LineageReporterFactory lineageReporterFactory =
        ReflectionUtil.getObj(lineageReporterFactoryClassName.get(), LineageReporterFactory.class);
    LineageReporter lineageReporter = lineageReporterFactory.getLineageReporter(config);

    lineageReporter.start();
    lineageReporter.report(lineageFactory.getLineage(config));
    lineageReporter.stop();

    LOGGER.info("Emitted lineage data to sink stream for job: {}", new ApplicationConfig(config).getAppName());
  }
}
