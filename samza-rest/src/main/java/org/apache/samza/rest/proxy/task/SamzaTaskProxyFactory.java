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
package org.apache.samza.rest.proxy.task;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ConfigFactory;
import org.apache.samza.rest.proxy.installation.InstallationFinder;
import org.apache.samza.rest.proxy.installation.SimpleInstallationFinder;
import org.apache.samza.rest.resources.BaseResourceConfig;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Creates the {@link TaskProxy} instances.
 */
public class SamzaTaskProxyFactory implements TaskProxyFactory {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaTaskProxyFactory.class);

  @Override
  public TaskProxy getTaskProxy(TaskResourceConfig config) {
    String installationsPath = config.getInstallationsPath();
    Preconditions.checkArgument(StringUtils.isNotEmpty(installationsPath),
                                String.format("Config param %s is not defined.", BaseResourceConfig.CONFIG_JOB_INSTALLATIONS_PATH));
    String configFactoryClass = config.getJobConfigFactory();
    try {
      ConfigFactory configFactory = Util.getObj(configFactoryClass, ConfigFactory.class);
      InstallationFinder installFinder = new SimpleInstallationFinder(installationsPath, configFactory);
      return new SamzaTaskProxy(config, installFinder);
    } catch (Exception e) {
      LOG.error(String.format("Exception during instantiation through configFactory class: %s.", configFactoryClass), e);
      throw new SamzaException(e);
    }
  }
}
