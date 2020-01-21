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

package org.apache.samza.runtime;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.ConfigLoaderFactory;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.util.CommandLine;


/**
 * This class contains the main() method used by run-app.sh.
 * It creates the {@link ApplicationRunner} based on the config, and then run the application.
 */
public class ApplicationRunnerMain {

  public static class ApplicationRunnerCommandLine extends CommandLine {
    public OptionSpec<String> operationOpt =
        parser().accepts("operation", "The operation to perform; run, status, kill.")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("operation=run")
            .defaultsTo("run");

    public ApplicationRunnerOperation getOperation(OptionSet options) {
      String rawOp = options.valueOf(operationOpt);
      return ApplicationRunnerOperation.fromString(rawOp);
    }

    @Override
    public Config loadConfig(OptionSet options) {
      // Verify legitimate parameters.
      if (!options.has(configLoaderPropertiesOpt())) {
        throw new ConfigException("Missing config loader properties");
      }

      // Set up the job parameters.
      Map<String, String> configLoaderFactory = Collections.singletonMap(JobConfig.CONFIG_LOADER_FACTORY, options.valueOf(configLoaderFactoryOpt()));
      Map<String, String> properties = options.valuesOf(configLoaderPropertiesOpt())
          .stream()
          .collect(Collectors.toMap(
              pair -> ConfigLoaderFactory.CONFIG_LOADER_PROPERTIES_PREFIX + pair.key,
              pair -> pair.value));
      Map<String, String> overrides = options.valuesOf(configOverrideOpt())
          .stream()
          .collect(Collectors.toMap(
              pair -> pair.key,
              pair -> pair.value));

      return new MapConfig(configLoaderFactory, properties, overrides);
    }
  }

  public static void main(String[] args) {
    ApplicationRunnerCommandLine cmdLine = new ApplicationRunnerCommandLine();
    OptionSet options = cmdLine.parser().parse(args);
    Config orgConfig = cmdLine.loadConfig(options);
    ApplicationRunnerOperation op = cmdLine.getOperation(options);
    ApplicationRunnerUtil.invoke(orgConfig, op);
  }
}
