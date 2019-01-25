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

package org.apache.samza.sql.util;

import java.util.HashMap;
import org.apache.samza.config.Config;
import org.apache.samza.sql.interfaces.SamzaRelTableKeyConverter;
import org.apache.samza.sql.interfaces.SamzaRelTableKeyConverterFactory;
import org.apache.samza.system.SystemStream;


/**
 * A sample {@link SamzaRelTableKeyConverterFactory} used in tests to create {@link SampleRelTableKeyConverter}.
 */
public class SampleRelTableKeyConverterFactory implements SamzaRelTableKeyConverterFactory {

  private final HashMap<SystemStream, SamzaRelTableKeyConverter> relConverters = new HashMap<>();

  @Override
  public SamzaRelTableKeyConverter create(SystemStream systemStream, Config config) {
    return relConverters.computeIfAbsent(systemStream, ss -> new SampleRelTableKeyConverter());
  }
}
