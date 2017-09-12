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

package org.apache.samza.sql.testutil;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;


/**
 * Utility methods to aid with config management.
 */
public class ConfigUtil {

  private ConfigUtil() {
  }

  /**
   * Method is used to filter just the properties with the prefix.
   * @param props Full set of properties
   * @param prefix Prefix of the keys that in the config that needs to be filtered out.
   * @param preserveFullKey If set to true, after filtering, preserves the full key including the prefix.
   *                        If set to false, Strips out the prefix from the key before returning.
   * @return Returns the filtered set of properties matching the prefix from the input property bag.
   */
  public static Properties getDomainProperties(Properties props, String prefix, boolean preserveFullKey) {
    String fullPrefix;
    if (StringUtils.isBlank(prefix)) {
      fullPrefix = ""; // this will effectively retrieve all properties
    } else {
      fullPrefix = prefix.endsWith(".") ? prefix : prefix + ".";
    }
    Properties ret = new Properties();
    props.keySet().stream().map(String.class::cast).forEach(keyStr -> {
      if (keyStr.startsWith(fullPrefix) && !keyStr.equals(fullPrefix)) {
        if (preserveFullKey) {
          ret.put(keyStr, props.get(keyStr));
        } else {
          ret.put(keyStr.substring(fullPrefix.length()), props.get(keyStr));
        }
      }
    });
    return ret;
  }
}
