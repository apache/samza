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

package org.apache.samza.sql.avro;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.samza.config.Config;
import org.apache.samza.sql.interfaces.RelSchemaProvider;
import org.apache.samza.sql.interfaces.RelSchemaProviderFactory;
import org.apache.samza.system.SystemStream;


/**
 * Avro Schema Resolver that uses static config to return a schema for a SystemStream.
 * Schemas are configured using the config of format {systemName}.{streamName}.schema.
 */
public class ConfigBasedAvroRelSchemaProviderFactory implements RelSchemaProviderFactory {

  public static final String CFG_SOURCE_SCHEMA = "%s.%s.schema";

  public RelSchemaProvider create(SystemStream systemStream, Config config) {
    return new ConfigBasedAvroRelSchemaProvider(systemStream, config);
  }

  public static class ConfigBasedAvroRelSchemaProvider implements AvroRelSchemaProvider {
    private final Config config;
    private final SystemStream systemStream;

    public ConfigBasedAvroRelSchemaProvider(SystemStream systemStream, Config config) {
      this.systemStream = systemStream;
      this.config = config;
    }

    public RelDataType getRelationalSchema() {
      String schemaStr = getSchema(systemStream);
      Schema schema = Schema.parse(schemaStr);
      AvroTypeFactoryImpl avroTypeFactory = new AvroTypeFactoryImpl();
      return avroTypeFactory.createType(schema);
    }

    @Override
    public String getSchema(SystemStream systemStream) {
      return config.get(String.format(CFG_SOURCE_SCHEMA, systemStream.getSystem(), systemStream.getStream()));
    }
  }
}
