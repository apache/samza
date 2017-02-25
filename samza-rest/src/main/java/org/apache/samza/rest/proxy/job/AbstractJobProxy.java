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
package org.apache.samza.rest.proxy.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ConfigFactory;
import org.apache.samza.config.factories.PropertiesConfigFactory;
import org.apache.samza.rest.model.Job;
import org.apache.samza.rest.model.JobStatus;
import org.apache.samza.rest.resources.JobsResourceConfig;
import org.apache.samza.util.ClassLoaderHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implements a subset of the {@link JobProxy} interface with the default, cluster-agnostic,
 * implementations. Subclasses are expected to override these default methods where necessary.
 */
public abstract class AbstractJobProxy implements JobProxy {
  private static final Logger log = LoggerFactory.getLogger(AbstractJobProxy.class);

  protected final JobsResourceConfig config;

  /**
   * Creates a new JobProxy instance from the factory class specified in the config.
   *
   * @param config  the config containing the job proxy factory property.
   * @return        the JobProxy produced by the factory.
   */
  public static JobProxy fromFactory(JobsResourceConfig config) {
    String jobProxyFactory = config.getJobProxyFactory();
    if (jobProxyFactory != null && !jobProxyFactory.isEmpty()) {
      try {
        JobProxyFactory factory = ClassLoaderHelper.<JobProxyFactory>fromClassName(jobProxyFactory);
        return factory.getJobProxy(config);
      } catch (Exception e) {
        throw new SamzaException(e);
      }
    } else {
      throw new SamzaException("Missing config: " + JobsResourceConfig.CONFIG_JOB_PROXY_FACTORY);
    }
  }

  /**
   * Required constructor.
   *
   * @param config  the config containing the installations path.
   */
  public AbstractJobProxy(JobsResourceConfig config) {
    this.config = config;
  }

  @Override
  public List<Job> getAllJobStatuses()
      throws IOException, InterruptedException {
    List<Job> allJobs = new ArrayList<>();
    Collection<JobInstance> jobInstances = getAllJobInstances();
    for(JobInstance jobInstance : jobInstances) {
        allJobs.add(new Job(jobInstance.getJobName(), jobInstance.getJobId()));
    }
    getJobStatusProvider().getJobStatuses(allJobs);

    return allJobs;
  }

  /**
   * Convenience method to get the Samza job status from the name and id.
   *
   * @param jobInstance           the instance of the job.
   * @return                      the current Samza status for the job.
   * @throws IOException          if there was a problem executing the command to get the status.
   * @throws InterruptedException if the thread was interrupted while waiting for the status result.
   */
  protected JobStatus getJobSamzaStatus(JobInstance jobInstance)
      throws IOException, InterruptedException {
    return getJobStatus(jobInstance).getStatus();
  }

  @Override
  public Job getJobStatus(JobInstance jobInstance)
      throws IOException, InterruptedException {
    return getJobStatusProvider().getJobStatus(jobInstance);
  }

  @Override
  public boolean jobExists(JobInstance jobInstance) {
    return getAllJobInstances().contains(jobInstance);
  }

  /**
   * @return the {@link ConfigFactory} to use to read job configuration files.
   */
  protected ConfigFactory getJobConfigFactory() {
    String configFactoryClassName = config.get(JobsResourceConfig.CONFIG_JOB_CONFIG_FACTORY);
    if (configFactoryClassName == null) {
      configFactoryClassName = PropertiesConfigFactory.class.getCanonicalName();
      log.warn("{} not specified. Defaulting to {}", JobsResourceConfig.CONFIG_JOB_CONFIG_FACTORY, configFactoryClassName);
    }

    try {
      return ClassLoaderHelper.<ConfigFactory>fromClassName(configFactoryClassName);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }
  /**
   * @return the {@link JobStatusProvider} to use in retrieving the job status.
   */
  protected abstract JobStatusProvider getJobStatusProvider();

  /**
   * @return all available job instances.
   */
  protected abstract Set<JobInstance> getAllJobInstances();

}
