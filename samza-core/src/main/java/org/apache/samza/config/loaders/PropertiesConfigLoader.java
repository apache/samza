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

package org.apache.samza.config.loaders;

import com.google.common.annotations.VisibleForTesting;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigLoader;
import org.apache.samza.config.MapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PropertiesConfigLoader implements ConfigLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PropertiesConfigLoader.class);
  private final Config config;

  @VisibleForTesting
  static final String PATH = "job.config.loader.properties.path";

  public PropertiesConfigLoader(Config config) {
    this.config = config;
  }

  @Override
  public Config getConfig() {
    String path = config.get(PATH);
    if (path == null) {
      throw new SamzaException(PATH + " is required to read config from properties file");
    }

    try {
      InputStream in = new FileInputStream(path);
      Properties props = new Properties();

      props.load(in);
      in.close();

      LOG.debug("got config {} from path {} with overrides {}", props, path, config);

      // Override loaded config values with provided overrides.
      return new MapConfig(new MapConfig(props), config);
    } catch (IOException e) {
      throw new SamzaException("Failed to read from");
    }
  }
}
