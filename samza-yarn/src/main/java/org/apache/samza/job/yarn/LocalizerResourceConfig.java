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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.samza.config.Config;

/**
 * LocalizerResourceConfigManager is intended to manage/fetch the config values
 * for the yarn localizer resource(s) from the configuration.
 *
 * There are 4 config values
 *   yarn.resources.&lt;resourceName&gt;.path
 *     (Required) The path for fetching the resource for localization,
 *     e.g. http://hostname.com/test.
 *   yarn.resources.&lt;resourceName&gt;.local.name
 *     (Optional) The local name used for the localized resource.
 *     If not set, the default one will be the &lt;resourceName&gt; from the config key.
 *   yarn.resources.&lt;resourceName&gt;.local.type
 *     (Optional) The value value is a string format of {@link LocalResourceType}:
 *     ARCHIVE, FILE, PATTERN.
 *     If not set, the default value is FILE.
 *   yarn.resources.&lt;resourceName&gt;.local.visibility
 *     (Optional) The valid value is a string format of {@link LocalResourceVisibility}:
 *     PUBLIC, PRIVATE, or APPLICATION.
 *     If not set, the default value is is APPLICATION.
 */
public class LocalizerResourceConfig {
  private static final String RESOURCE_PREFIX = "yarn.resources.";
  private static final String PATH_SUFFIX = ".path";
  private static final String RESOURCE_PATH = "yarn.resources.%s.path";
  private static final String RESOURCE_LOCAL_NAME = "yarn.resources.%s.local.name";
  private static final String RESOURCE_LOCAL_TYPE = "yarn.resources.%s.local.type";
  private static final String RESOURCE_LOCAL_VISIBILITY = "yarn.resources.%s.local.visibility";
  private static final String DEFAULT_RESOURCE_LOCAL_TYPE = "FILE";
  private static final String DEFAULT_RESOURCE_LOCAL_VISIBILITY = "APPLICATION";

  private final Config config;

  public LocalizerResourceConfig(final Config config) {
    if (null == config) {
      throw new IllegalArgumentException("config cannot be null");
    }
    this.config = config;
  }

  public List<String> getResourceNames() {
    Config subConfig = config.subset(RESOURCE_PREFIX, true);
    List<String> resourceNames = new ArrayList<String>();
    for (String key : subConfig.keySet()) {
      if (key.endsWith(PATH_SUFFIX)) {
        resourceNames.add(key.substring(0, key.length() - PATH_SUFFIX.length()));
      }
    }
    return resourceNames;
  }

  public Path getResourcePath(final String resourceName) {
    String pathStr = config.get(String.format(RESOURCE_PATH, resourceName));
    if (StringUtils.isEmpty(pathStr)) {
      throw new LocalizerResourceException("resource path is required but not defined in config for resource " + resourceName);
    }
    return new Path(pathStr);
  }

  public LocalResourceType getResourceLocalType(final String resourceName) {
    String typeStr = config.get(String.format(RESOURCE_LOCAL_TYPE, resourceName), DEFAULT_RESOURCE_LOCAL_TYPE);
    return LocalResourceType.valueOf(StringUtils.upperCase(typeStr));
  }

  public LocalResourceVisibility getResourceLocalVisibility(final String resourceName) {
    String visibilityStr = config.get(String.format(RESOURCE_LOCAL_VISIBILITY, resourceName), DEFAULT_RESOURCE_LOCAL_VISIBILITY);
    return LocalResourceVisibility.valueOf(StringUtils.upperCase(visibilityStr));
  }

  public String getResourceLocalName(final String resourceName) {
    String name = config.get(String.format(RESOURCE_LOCAL_NAME, resourceName), resourceName);
    return name;
  }

}
