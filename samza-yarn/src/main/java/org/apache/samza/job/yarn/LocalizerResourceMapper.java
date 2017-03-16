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
package org.apache.samza.job.yarn;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.YarnConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A universal approach to generate local resource map which can be put in ContainerLaunchContext directly
 */
public class LocalizerResourceMapper {
  private static final Logger log = LoggerFactory.getLogger(LocalizerResourceMapper.class);

  private Config config; //job configurations
  private YarnConfiguration yarnConfiguration; //yarn configurations
  private Map<String, LocalResource> localResourceMap;

  public LocalizerResourceMapper(Config config, YarnConfiguration yarnConfiguration) {
    this.config = config;
    this.yarnConfiguration = yarnConfiguration;
    this.localResourceMap = new HashMap<>();
  }

  public void map() throws LocalizerResourceException {
    // filter to get localizer resource config
    Config localizerResourceConfig;
    try {
      YarnConfig yarnConfig = new YarnConfig(config);
      localizerResourceConfig = yarnConfig.getLocalizerResourceConfigs();
      log.info("localizerResourceConfig: {} ", localizerResourceConfig);
    } catch (SamzaException e) {
      log.info("localizerResourceConfig not found in config");
      return;
    }

    // create LocalResource and put in map
    for (Map.Entry<String, String> entry : localizerResourceConfig.entrySet()) {
      LocalizerResourceConfigParser resourceConfigParser = new LocalizerResourceConfigParser(entry.getKey(), entry.getValue());

      if (!resourceConfigParser.isValid()) {
        throw new LocalizerResourceException("resourceConfigParser fails. Resource visibility, type, name or path is missing or invalid for " + entry);
      }

      Path resourcePath = resourceConfigParser.getResourcePath();

      LocalResource localResource = createLocalResource(resourcePath, resourceConfigParser.getResourceType(), resourceConfigParser.getResourceVisibility());

      localResourceMap.put(resourceConfigParser.getResourceName(), localResource);
      log.info("preparing local resource: {}", resourceConfigParser.getResourceName());
    }

  }

  private LocalResource createLocalResource(Path resourcePath, LocalResourceType resourceType, LocalResourceVisibility resourceVisibility) throws LocalizerResourceException {
    LocalResource localResource = Records.newRecord(LocalResource.class);
    URL resourceUrl = ConverterUtils.getYarnUrlFromPath(resourcePath);
    try {
      FileStatus resourceFileStatus = resourcePath.getFileSystem(yarnConfiguration).getFileStatus(resourcePath);

      if (null == resourceFileStatus) {
        throw new LocalizerResourceException("Check getFileStatus implementation. getFileStatus gets unexpected null for resourcePath " + resourcePath);
      }

      localResource.setResource(resourceUrl);
      log.info("setLocalizerResource for {}", resourceUrl);
      localResource.setSize(resourceFileStatus.getLen());
      localResource.setTimestamp(resourceFileStatus.getModificationTime());
      localResource.setType(resourceType);
      localResource.setVisibility(resourceVisibility);
      return localResource;
    } catch (IOException ioe) {
      log.error("IO Exception when accessing the resource file status from the filesystem", ioe);
      throw new LocalizerResourceException("IO Exception when accessing the resource file status from the filesystem");
    }

  }

  public Map<String, LocalResource> getResourceMap() {
    return localResourceMap;
  }

}
