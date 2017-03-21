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
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * FsImplConfigManager is intented to manage the Samza config for fs.&lt;scheme&gt;impl.
 * e.g. fs.http.impl
 *
 * Usually this config exist in yarn config,
 * but it is also allowed the clients define this for Samza Job and pick specific implementation for the job.
 */
public class FsImplConfigManager {
  private static final Logger log = LoggerFactory.getLogger(FsImplConfigManager.class);
  private static final String FS_IMPL_PREFIX = "fs.";
  private static final String FS_IMPL_SUFFIX = ".impl";
  private static final String FS_IMPL = "fs.%s.impl";

  private final Config config;

  public FsImplConfigManager(final Config config) {
    if (null == config) {
      this.config = new MapConfig();
    } else {
      this.config = config;
    }
  }

  /**
   * Get all schemes
   * @return List of schemes in strings
   */
  public List<String> getSchemes() {
    Config subConfig = config.subset(FS_IMPL_PREFIX, true);
    List<String> schemes = new ArrayList<String>();
    for (String key : subConfig.keySet()) {
      if (key.endsWith(FS_IMPL_SUFFIX)) {
        schemes.add(key.substring(0, key.length() - FS_IMPL_SUFFIX.length()));
      }
    }
    return schemes;
  }

  /**
   * Get the fs.&lt;scheme&gt;impl as the config key from scheme
   * @param scheme scheme name, such as http, hdfs, myscheme
   * @return fs.&lt;scheme&gt;impl
   */
  public String getFsImplKey(final String scheme) {
    String fsImplKey = String.format(FS_IMPL, scheme);
    return fsImplKey;
  }

  /**
   * Get the class name corresponding for the given scheme
   * @param scheme scheme name, such as http, hdfs, myscheme
   * @return full scoped class name for the file system for &lt;scheme&gt;
   * @throws LocalizerResourceException
   */
  public String getFsImplClassName(final String scheme) throws LocalizerResourceException {
    String fsImplKey = getFsImplKey(scheme);
    String fsImplClassName = config.get(fsImplKey);
    if (StringUtils.isEmpty(fsImplClassName)) {
      throw new LocalizerResourceException(fsImplKey + " does not have configured class implementation");
    }
    return fsImplClassName;
  }

  /**
   * Take the fs.&lt;scheme&gt;impl from the samza job configuration and override the values in yarn configuration
   * @param yarnConfiguration
   */
  public void overrideYarnConfiguration(YarnConfiguration yarnConfiguration) {
    if (null == yarnConfiguration) {
      log.error("yarnConfiguration is null, skipping overrideYarnConfiguration");
      return;
    }
    try {
      List<String> schemes = getSchemes();
      for (String scheme : schemes) {
        String fsImplKey = getFsImplKey(scheme);
        yarnConfiguration.set(fsImplKey, getFsImplClassName(scheme));
        log.info("Samza job config {} is used for yarn.", fsImplKey);
      }
    } catch (LocalizerResourceException e) {
      log.error("Exception when overriding fs.<scheme>.impl from Samza jobs, ignoring by defaulting to yarn config. ", e);
    }
  }
}
