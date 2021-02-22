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
package org.apache.samza.clustermanager;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.classloader.IsolatingClassLoaderFactory;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ShellCommandConfig;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.SplitDeploymentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClusterBasedJobCoordinatorRunner {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterBasedJobCoordinatorRunner.class);

  /**
   * The entry point for the {@link ClusterBasedJobCoordinator}.
   */
  public static void main(String[] args) {
    Thread.setDefaultUncaughtExceptionHandler((thread, exception) -> {
      LOG.error("Uncaught exception in ClusterBasedJobCoordinator::main. Exiting job coordinator", exception);
      System.exit(1);
    });
    if (!SplitDeploymentUtil.isSplitDeploymentEnabled()) {
      // no isolation enabled, so can just execute runClusterBasedJobCoordinator directly
      runClusterBasedJobCoordinator(args);
    } else {
      SplitDeploymentUtil.runWithClassLoader(new IsolatingClassLoaderFactory().buildClassLoader(),
          ClusterBasedJobCoordinatorRunner.class, "runClusterBasedJobCoordinator", args);
    }
    System.exit(0);
  }

  /**
   * This is the actual execution for the {@link ClusterBasedJobCoordinator}. This is separated out from
   * {@link #main(String[])} so that it can be executed directly or from a separate classloader.
   */
  @VisibleForTesting
  static void runClusterBasedJobCoordinator(String[] args) {
    final String coordinatorSystemEnv = System.getenv(ShellCommandConfig.ENV_COORDINATOR_SYSTEM_CONFIG);
    final String submissionEnv = System.getenv(ShellCommandConfig.ENV_SUBMISSION_CONFIG);

    if (!StringUtils.isBlank(submissionEnv)) {
      Config submissionConfig;
      try {
        //Read and parse the coordinator system config.
        LOG.info("Parsing submission config {}", submissionEnv);
        submissionConfig =
            new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(submissionEnv, Config.class));
        LOG.info("Using the submission config: {}.", submissionConfig);
      } catch (IOException e) {
        LOG.error("Exception while reading submission config", e);
        throw new SamzaException(e);
      }

      ApplicationConfig appConfig = new ApplicationConfig(submissionConfig);

      /*
       * Invoke app.main.class with app.main.args when present.
       * For Beam jobs, app.main.class will be Beam's main class
       * and app.main.args will be Beam's pipeline options.
       */
      String className = appConfig.getAppMainClass();
      String[] arguments = toArgs(appConfig);
      LOG.info("Invoke main {} with args {}", className, arguments);
      try {
        Class<?> cls = Class.forName(className);
        Method mainMethod = cls.getMethod("main", String[].class);
        mainMethod.invoke(null, (Object) arguments);
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    } else {
      // TODO: Clean this up once SAMZA-2405 is completed when legacy flow is removed.
      Config coordinatorSystemConfig;
      try {
        //Read and parse the coordinator system config.
        LOG.info("Parsing coordinator system config {}", coordinatorSystemEnv);
        coordinatorSystemConfig =
            new MapConfig(SamzaObjectMapper.getObjectMapper().readValue(coordinatorSystemEnv, Config.class));
        LOG.info("Using the coordinator system config: {}.", coordinatorSystemConfig);
      } catch (IOException e) {
        LOG.error("Exception while reading coordinator stream config", e);
        throw new SamzaException(e);
      }
      ClusterBasedJobCoordinator jc = createFromMetadataStore(coordinatorSystemConfig);
      jc.run();
    }

    LOG.info("Finished running ClusterBasedJobCoordinator");
  }

  /**
   * Initialize {@link ClusterBasedJobCoordinator} with coordinator stream config, full job config will be fetched from
   * coordinator stream.
   *
   * @param metadataStoreConfig to initialize {@link org.apache.samza.metadatastore.MetadataStore}
   * @return {@link ClusterBasedJobCoordinator}
   */
  // TODO SAMZA-2432: Clean this up once SAMZA-2405 is completed when legacy flow is removed.
  @VisibleForTesting
  static ClusterBasedJobCoordinator createFromMetadataStore(Config metadataStoreConfig) {
    MetricsRegistryMap metrics = new MetricsRegistryMap();

    CoordinatorStreamStore coordinatorStreamStore = new CoordinatorStreamStore(metadataStoreConfig, metrics);
    coordinatorStreamStore.init();
    Config config = CoordinatorStreamUtil.readConfigFromCoordinatorStream(coordinatorStreamStore);

    return new ClusterBasedJobCoordinator(metrics, coordinatorStreamStore, config);
  }

  /**
   * Convert Samza config to command line arguments to invoke app.main.class
   *
   * @param config Samza config to convert.
   * @return converted command line arguments.
   */
  @VisibleForTesting
  static String[] toArgs(ApplicationConfig config) {
    List<String> args = new ArrayList<>(config.size() * 2);

    config.forEach((key, value) -> {
      if (key.equals(ApplicationConfig.APP_MAIN_ARGS)) {
        /*
         * Converts native beam pipeline options such as
         * --runner=SamzaRunner --maxSourceParallelism=1024
         */
        args.addAll(Arrays.asList(value.split("\\s")));
      } else {
        /*
         * Converts native Samza configs to config override format such as
         * --config job.name=test
         */
        args.add("--config");
        args.add(String.format("%s=%s", key, value));
      }
    });

    return args.toArray(new String[0]);
  }
}
