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

package org.apache.samza.sql.interfaces;

import org.apache.commons.lang.Validate;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;


/**
 * Configs associated with a system stream.
 */
public class SqlSystemStreamConfig {

  public static final String CFG_SAMZA_REL_CONVERTER = "samzaRelConverterName";
  public static final String CFG_REL_SCHEMA_PROVIDER = "relSchemaProviderName";

  private final String systemName;

  private final String streamName;

  private final String samzaRelConverterName;
  private final SystemStream systemStream;
  private String relSchemaProviderName;

  public SqlSystemStreamConfig(String systemName, String streamName, Config systemConfig) {

    this.systemName = systemName;
    this.streamName = streamName;
    this.systemStream = new SystemStream(systemName, streamName);

    samzaRelConverterName = systemConfig.get(CFG_SAMZA_REL_CONVERTER);
    relSchemaProviderName = systemConfig.get(CFG_REL_SCHEMA_PROVIDER);
    Validate.notEmpty(samzaRelConverterName,
        String.format("%s is not set or empty for system %s", CFG_SAMZA_REL_CONVERTER, systemName));
  }

  public String getSystemName() {
    return systemName;
  }

  public String getStreamName() {
    return streamName;
  }

  public String getSamzaRelConverterName() {
    return samzaRelConverterName;
  }

  public String getRelSchemaProviderName() {
    return relSchemaProviderName;
  }

  public SystemStream getSystemStream() {
    return systemStream;
  }
}
