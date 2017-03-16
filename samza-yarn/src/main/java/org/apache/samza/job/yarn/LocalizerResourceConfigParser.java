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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.samza.clustermanager.*;
import org.apache.samza.config.YarnConfig;


/**
 * LocalizerResourceConfigParser is intended to parse the config key and value
 * for the localizer resource(s) from the configuration.
 *
 * The general format of the localizer resource will be the following:
 * key: localizer.resource.&lt;resourceVisibility&gt;.&lt;resourceType&gt;.&lt;resourceName&gt;
 *   e.g. localizer.resource.APPLICATION.FILE.identity
 *   where the valid resourceVisibility is a string format of {@link LocalResourceVisibility}:
 *   PUBLIC, PRIVATE, or APPLICATION;
 *   and the valid resourceType is a string format of {@link LocalResourceType}:
 *   ARCHIVE, FILE, PATTERN;
 *   and the valid resourceName could be any non empty {@link String}.
 * value: <resourcePath> in a string format. e.g. http://hostname.com/test
 *
 */
public class LocalizerResourceConfigParser {
  private LocalResourceVisibility visibility;
  private LocalResourceType type;
  private String name;
  private Path path;

  public LocalizerResourceConfigParser(String resourceConfKey, String resourceConfValue) throws LocalizerResourceException {
    parseConfKey(resourceConfKey);
    parseConfValue(resourceConfValue);
  }

  private void parseConfKey(String resourceConfKey) throws LocalizerResourceException {
    //parse resource key to get resource visibility, resource type, resource name
    String regexStr = YarnConfig.LOCALIZER_RESOURCE_PREFIX + "([^\\.]*)\\.([^\\.]*)\\.(.*)";
    Pattern pattern = Pattern.compile(regexStr);
    Matcher matcher = pattern.matcher(resourceConfKey);
    if (matcher.find( )) {//the config matches localizer.resource.<visibility>.<type>.<resourceName>
      String resourceVisibilityStr = StringUtils.upperCase(matcher.group(1)); //visibility shows only as upper case
      String resourceTypeStr = StringUtils.upperCase(matcher.group(2)); //type shows only as upper case
      try {
        this.visibility = LocalResourceVisibility.valueOf(resourceVisibilityStr);
        this.type = LocalResourceType.valueOf(resourceTypeStr);
      } catch (IllegalArgumentException e) {
        throw new LocalizerResourceException("Invalid resource visibility or type from localizer resource config key: (" + resourceConfKey + ").", e);
      }
      this.name = matcher.group(3);
    } else {
      throw new LocalizerResourceException("Invalid localizer resource config key: (" + resourceConfKey + "). The valid syntax is localizer.resource.<visibility>.<type>.<resourceName>");
    }

  }

  private void parseConfValue(String resourceConfValue) throws LocalizerResourceException {
    try {
      this.path = new Path(resourceConfValue);
    } catch (IllegalArgumentException e) {
      throw new LocalizerResourceException("Invalid localizer resource config value: (" + resourceConfValue + ").", e);
    }
  }

  public boolean isValid() {
    if (null == this.visibility
        || null == this.type
        || StringUtils.isEmpty(this.name)
        || null == this.path ) {
      return false;
    }
    return true;
  }

  public LocalResourceVisibility getResourceVisibility() {
    return visibility;
  }

  public LocalResourceType getResourceType() {
    return type;
  }

  public String getResourceName() {
    return name;
  }

  public Path getResourcePath() {
    return path;
  }
}
