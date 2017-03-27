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
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * FileSystemImplConfig is intended to manage the Samza config for fs.&lt;scheme&gt;impl.
 * e.g. fs.http.impl
 */
public class FileSystemImplConfig {
  private static final String FS_IMPL_PREFIX = "fs.";
  private static final String FS_IMPL_SUFFIX = ".impl";
  private static final String FS_IMPL_TEMPLATE = "fs.%s.impl";

  private final Config config;

  public FileSystemImplConfig(final Config config) {
    if (null == config) {
      throw new IllegalArgumentException("config cannot be null");
    }
    this.config = config;
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
   * Get the config subset for fs.&lt;scheme&gt;.impl
   * It can include config for fs.&lt;scheme&gt;.impl and additional config for the subKeys fs.&lt;scheme&gt;.impl.* from the configuration
   * e.g. for scheme "myScheme", there could be config for fs.myScheme.impl, fs.myScheme.impl.client and fs.myScheme.impl.server
   * @param scheme scheme name, such as http, hdfs, myscheme
   * @return config for the particular scheme
   */
  public Config getSchemeConfig(final String scheme) {
    String fsSchemeImpl = String.format(FS_IMPL_TEMPLATE, scheme);
    Config schemeConfig = config.subset(fsSchemeImpl, false); // do not strip off the prefix
    return schemeConfig;
  }

  /**
   * Get the set of subKeys for fs.&lt;scheme&gt;.impl if .&lt;scheme&gt;. has additional subKeys in the configuration
   * e.g. fs.myScheme.impl.client and fs.myScheme.impl.server are the subKeys for myScheme
   * @param scheme scheme name, such as http, hdfs, myscheme
   * @return a set of subKeys from the configuration without stripping off prefix
   */
  public Set<String> getFsImplSubKeys(final String scheme) {
    String fsImplSubKeyPrefix = String.format(FS_IMPL_SUBKEY_PREFIX, scheme);
    Config subConfig = config.subset(fsImplSubKeyPrefix, false); // do not strip off the prefix
    return subConfig.keySet();
  }
}
