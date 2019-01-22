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

package org.apache.samza.sql.client.impl;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.client.interfaces.ExecutionContext;
import org.apache.samza.sql.schema.SqlSchema;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.samza.sql.client.impl.SamzaExecutor.*;
import static org.apache.samza.sql.runner.SamzaSqlApplicationConfig.*;


public class SamzaExecutorTest {
    private SamzaExecutor m_executor = new SamzaExecutor();

    @Test
    public void testGetTableSchema() {
        ExecutionContext context = getExecutionContext();
        SqlSchema ts = m_executor.getTableSchema(context, "kafka.ProfileChangeStream");

        List<SqlSchema.SqlField> fields = ts.getFields();
        Assert.assertEquals("Name", fields.get(0).getFieldName());
        Assert.assertEquals("NewCompany", fields.get(1).getFieldName());
        Assert.assertEquals("OldCompany", fields.get(2).getFieldName());
        Assert.assertEquals("ProfileChangeTimestamp", fields.get(3).getFieldName());
        Assert.assertEquals("STRING", fields.get(0).getFieldSchema().getFieldType().toString());
        Assert.assertEquals("STRING", fields.get(1).getFieldSchema().getFieldType().toString());
        Assert.assertEquals("STRING", fields.get(2).getFieldSchema().getFieldType().toString());
        Assert.assertEquals("INT64", fields.get(3).getFieldSchema().getFieldType().toString());
    }

    // Generate result schema needs to be fixed. SAMZA-2079
    @Ignore
    @Test
    public void testGenerateResultSchema() {
        ExecutionContext context = getExecutionContext();
        Map<String, String> mapConf = fetchSamzaSqlConfig(1, context);
        SqlSchema ts = m_executor.generateResultSchema(new MapConfig(mapConf));

        List<SqlSchema.SqlField> fields = ts.getFields();
        Assert.assertEquals("__key__", fields.get(0).getFieldName());
        Assert.assertEquals("Name", fields.get(1).getFieldName());
        Assert.assertEquals("NewCompany", fields.get(2).getFieldName());
        Assert.assertEquals("OldCompany", fields.get(3).getFieldName());
        Assert.assertEquals("ProfileChangeTimestamp", fields.get(4).getFieldName());
        Assert.assertEquals("ANY", fields.get(0).getFieldSchema().getFieldType().toString());
        Assert.assertEquals("VARCHAR", fields.get(1).getFieldSchema().getFieldType().toString());
        Assert.assertEquals("VARCHAR", fields.get(2).getFieldSchema().getFieldType().toString());
        Assert.assertEquals("VARCHAR", fields.get(3).getFieldSchema().getFieldType().toString());
        Assert.assertEquals("BIGINT", fields.get(4).getFieldSchema().getFieldType().toString());
    }

    private ExecutionContext getExecutionContext() {
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource("ProfileChangeStream.avsc").getFile());
        Map<String, String> mapConf = new HashMap<>();
        mapConf.put("samza.sql.relSchemaProvider.config.schemaDir", file.getParent());
        mapConf.put(CFG_SQL_STMT, "insert into log.outputStream select * from kafka.ProfileChangeStream");
        return new ExecutionContext(mapConf);
    }
}
