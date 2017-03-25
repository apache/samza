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

package org.apache.samza.validation;

import java.util.Map;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptReport;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.YarnApplicationAttemptState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.stream.messages.SetContainerHostMapping;
import org.apache.samza.job.yarn.ClientHelper;
import org.apache.samza.metrics.JmxMetricsAccessor;
import org.apache.samza.metrics.MetricsValidator;
import org.apache.samza.util.ClassLoaderHelper;
import org.apache.samza.util.hadoop.HttpFileSystem;
import org.apache.samza.util.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line tool for validating the status of a Yarn job.
 * It checks the job has been successfully submitted to the Yarn cluster, the status of
 * the application attempt is running and the running container count matches the expectation.
 * It also supports an optional MetricsValidator plugin through arguments so job metrics can
 * be validated too using JMX. This tool can be used, for example, as an automated validation
 * step after starting a job.
 *
 * When running this tool, please provide the configuration URI of job. For example:
 *
 * deploy/samza/bin/validate-yarn-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties [--metrics-validator=com.foo.bar.SomeMetricsValidator]
 *
 * The tool prints out the validation result in each step and throws an exception when the
 * validation fails.
 */
public class YarnJobValidationTool {
  private static final Logger log = LoggerFactory.getLogger(YarnJobValidationTool.class);

  private final JobConfig config;
  private final YarnClient client;
  private final String jobName;
  private final MetricsValidator validator;

  public YarnJobValidationTool(JobConfig config, YarnClient client, MetricsValidator validator) {
    this.config = config;
    this.client = client;
    String name = this.config.getName().get();
    String jobId = this.config.getJobId().nonEmpty()? this.config.getJobId().get() : "1";
    this.jobName =  name + "_" + jobId;
    this.validator = validator;
  }

  public void run() {
    ApplicationId appId;
    ApplicationAttemptId attemptId;

    try {
      log.info("Start validating job " + this.jobName);

      appId = validateAppId();
      attemptId = validateRunningAttemptId(appId);
      validateContainerCount(attemptId);
      if(validator != null) {
        validateJmxMetrics();
      }

      log.info("End of validation");
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      System.exit(1);
    }
  }

  public ApplicationId validateAppId() throws Exception {
    // fetch only the last created application with the job name and id
    // i.e. get the application with max appId
    ApplicationId appId = null;
    for(ApplicationReport applicationReport : this.client.getApplications()) {
      if(applicationReport.getName().equals(this.jobName)) {
        ApplicationId id = applicationReport.getApplicationId();
        if(appId == null || appId.compareTo(id) < 0) {
          appId = id;
        }
      }
    }
    if (appId != null) {
      log.info("Job lookup success. ApplicationId " + appId.toString());
      return appId;
    } else {
      throw new SamzaException("Job lookup failure " + this.jobName);
    }
  }

  public ApplicationAttemptId validateRunningAttemptId(ApplicationId appId) throws Exception {
    ApplicationAttemptId attemptId = this.client.getApplicationReport(appId).getCurrentApplicationAttemptId();
    ApplicationAttemptReport attemptReport = this.client.getApplicationAttemptReport(attemptId);
    if (attemptReport.getYarnApplicationAttemptState() == YarnApplicationAttemptState.RUNNING) {
      log.info("Job is running. AttempId " + attemptId.toString());
      return attemptId;
    } else {
      throw new SamzaException("Job not running " + this.jobName);
    }
  }

  public int validateContainerCount(ApplicationAttemptId attemptId) throws Exception {
    int runningContainerCount = 0;
    for(ContainerReport containerReport : this.client.getContainers(attemptId)) {
      if(containerReport.getContainerState() == ContainerState.RUNNING) {
        ++runningContainerCount;
      }
    }
    // expected containers to be the configured job containers plus the AppMaster container
    int containerExpected = this.config.getContainerCount() + 1;

    if (runningContainerCount == containerExpected) {
      log.info("Container count matches. " + runningContainerCount + " containers are running.");
      return runningContainerCount;
    } else {
      throw new SamzaException("Container count does not match. " + runningContainerCount + " containers are running, while " + containerExpected + " is expected.");
    }
  }

  public void validateJmxMetrics() throws Exception {
    JobModelManager jobModelManager = JobModelManager.apply(config);
    validator.init(config);
    Map<String, String> jmxUrls = jobModelManager.jobModel().getAllContainerToHostValues(SetContainerHostMapping.JMX_TUNNELING_URL_KEY);
    for (Map.Entry<String, String> entry : jmxUrls.entrySet()) {
      String containerId = entry.getKey();
      String jmxUrl = entry.getValue();
      log.info("validate container " + containerId + " metrics with JMX: " + jmxUrl);
      JmxMetricsAccessor jmxMetrics = new JmxMetricsAccessor(jmxUrl);
      jmxMetrics.connect();
      validator.validate(jmxMetrics);
      jmxMetrics.close();
      log.info("validate container " + containerId + " successfully");
    }
    validator.complete();
  }

  public static void main(String [] args) throws Exception {
    CommandLine cmdline = new CommandLine();
    OptionParser parser = cmdline.parser();
    OptionSpec<String> validatorOpt = parser.accepts("metrics-validator", "The metrics validator class.")
                                            .withOptionalArg()
                                            .ofType(String.class).describedAs("com.foo.bar.ClassName");
    OptionSet options = cmdline.parser().parse(args);
    Config config = cmdline.loadConfig(options);
    MetricsValidator validator = null;
    if (options.has(validatorOpt)) {
      String validatorClass = options.valueOf(validatorOpt);
      validator = ClassLoaderHelper.<MetricsValidator>fromClassName(validatorClass);
    }

    YarnConfiguration hadoopConfig = new YarnConfiguration();
    hadoopConfig.set("fs.http.impl", HttpFileSystem.class.getName());
    hadoopConfig.set("fs.https.impl", HttpFileSystem.class.getName());
    ClientHelper clientHelper = new ClientHelper(hadoopConfig);

    new YarnJobValidationTool(new JobConfig(config), clientHelper.yarnClient(), validator).run();
  }
}