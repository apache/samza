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
package org.apache.samza.sql.calcite.schema;

import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

public class TestAvroSchemaConverter {
  public static final String SIMPLE_RECORD_SCHEMA = "{\"namespace\": \"example.avro\",\n" +
      " \"type\": \"record\",\n" +
      " \"name\": \"User\",\n" +
      " \"fields\": [\n" +
      "     {\"name\": \"name\", \"type\": \"string\"},\n" +
      "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
      "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
      " ]\n" +
      "}";

  public static final Schema simpleRecord = new Schema.Parser().parse(SIMPLE_RECORD_SCHEMA);
  @Test
  public void testSimpleAvroRecord(){
    RelDataTypeFactory relDataTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    AvroSchemaConverter schemaConverter = new AvroSchemaConverter(relDataTypeFactory, simpleRecord);

    RelDataType relDataType = schemaConverter.convert();

    Assert.assertEquals(SqlTypeName.VARCHAR, relDataType.getField("name", false, false).getType().getSqlTypeName());
    Assert.assertEquals(SqlTypeName.INTEGER, relDataType.getField("favorite_number", false, false).getType().getSqlTypeName());
    Assert.assertTrue(relDataType.getField("favorite_number", false, false).getType().isNullable());
    Assert.assertEquals(SqlTypeName.VARCHAR, relDataType.getField("favorite_color", false, false).getType().getSqlTypeName());
    Assert.assertTrue(relDataType.getField("favorite_color", false, false).getType().isNullable());
  }
}
