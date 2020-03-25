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

import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.avro.AvroRelConverter;
import org.apache.samza.sql.avro.AvroRelSchemaProvider;
import org.apache.samza.sql.interfaces.RelSchemaProvider;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SamzaRelConverterFactory;
import org.apache.samza.system.SystemStream;


/**
 * SampleRelConverter is an {@link AvroRelConverter} which identifies alternate messages as system messages.
 * This is used purely for testing system messages.
 */
public class SampleRelConverterFactory implements SamzaRelConverterFactory {

  private int i = 0;

  @Override
  public SamzaRelConverter create(SystemStream systemStream, RelSchemaProvider relSchemaProvider, Config config) {
    return new SampleRelConverter(systemStream, (AvroRelSchemaProvider) relSchemaProvider, config);
  }

  public class SampleRelConverter extends AvroRelConverter {
    public SampleRelConverter(SystemStream systemStream, AvroRelSchemaProvider schemaProvider, Config config) {
      super(systemStream, schemaProvider, config);
    }

    @Override
    public boolean isSystemMessage(KV<Object, Object> kv) {
      // Return alternate ones as system messages.
      return (i++) % 2 == 0;
    }
  }
}
