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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A universal approach to generate local resource map which can be put in ContainerLaunchContext directly
 */
public class LocalizerResourceMapper {
  private static final Logger log = LoggerFactory.getLogger(LocalizerResourceMapper.class);

  private Config config; //job configurations
  private YarnConfiguration yarnConfiguration; //yarn configurations
  private LocalizerResourceConfigManager resourceConfigManager;
  private ImmutableMap<String, LocalResource> localResourceMap;

  public LocalizerResourceMapper(Config config, YarnConfiguration yarnConfiguration) throws LocalizerResourceException {
    this.config = config;
    this.yarnConfiguration = yarnConfiguration;
    this.resourceConfigManager = new LocalizerResourceConfigManager(this.config);
    map(); // set up localResourceMap
  }

  private void map() throws LocalizerResourceException {
    ImmutableMap.Builder<String, LocalResource>  localResourceMapBuilder = ImmutableMap.builder();

    List<String> resourceNames = resourceConfigManager.getResourceNames();
    for (String resourceName : resourceNames) {
      String resourceLocalName = resourceConfigManager.getResourceLocalName(resourceName);
      LocalResourceType resourceType = resourceConfigManager.getResourceLocalType(resourceName);
      LocalResourceVisibility resourceVisibility = resourceConfigManager.getResourceLocalVisibility(resourceName);
      Path resourcePath = resourceConfigManager.getResourcePath(resourceName);

      LocalResource localResource = createLocalResource(resourcePath, resourceType, resourceVisibility);

      localResourceMapBuilder.put(resourceLocalName, localResource);
      log.info("preparing local resource: {}", resourceLocalName);
    }

    this.localResourceMap = localResourceMapBuilder.build();
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
      log.error("IO Exception when accessing the resource file status from the filesystem: " + resourcePath, ioe);
      throw new LocalizerResourceException("IO Exception when accessing the resource file status from the filesystem: " + resourcePath);
    }

  }

  public ImmutableMap<String, LocalResource> getResourceMap() {
    return localResourceMap;
  }

}
