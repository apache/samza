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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigLoader;
import org.apache.samza.config.MapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;


/**
 * ConfigLoader to load full job configs from a local properties file given its path.
 */
public class PropertiesConfigLoader implements ConfigLoader {
  private static final Logger LOG = LoggerFactory.getLogger(PropertiesConfigLoader.class);

  private final String path;

  public PropertiesConfigLoader(String path) {
    this.path = requireNonNull(path);
  }

  @Override
  public Config getConfig() {
    try {
      InputStream in = new FileInputStream(path);
      Properties props = new Properties();

      props.load(in);
      in.close();

      LOG.debug("got config {} from path {}", props, path);

      Map<String, String> config = new HashMap<>(props.size());
      // Per Properties JavaDoc, all its keys and values are of type String
      props.forEach((key, value) -> config.put(key.toString(), value.toString()));

      return new MapConfig(config);
    } catch (IOException e) {
      throw new SamzaException("Failed to read from " + path);
    }
  }
}