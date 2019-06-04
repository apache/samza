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

package org.apache.samza.table;

import java.util.Arrays;
import java.util.List;

import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.table.descriptors.LocalTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;


public class TestTableConfigGenerator {
  @Test
  public void testWithSerdes() {
    List<TableDescriptor> descriptors = Arrays.asList(
        new MockLocalTableDescriptor("t1", KVSerde.of(new StringSerde(), new IntegerSerde())),
        new MockLocalTableDescriptor("t2", KVSerde.of(new StringSerde(), new IntegerSerde()))
    );
    Config jobConfig = new MapConfig(TableConfigGenerator.generateSerdeConfig(descriptors));
    JavaTableConfig javaTableConfig = new JavaTableConfig(jobConfig);
    assertNotNull(javaTableConfig.getKeySerde("t1"));
    assertNotNull(javaTableConfig.getMsgSerde("t1"));
    assertNotNull(javaTableConfig.getKeySerde("t2"));
    assertNotNull(javaTableConfig.getMsgSerde("t2"));

    MapConfig tableConfig = new MapConfig(TableConfigGenerator.generate(jobConfig, descriptors));
    javaTableConfig = new JavaTableConfig(tableConfig);
    assertNotNull(javaTableConfig.getTableProviderFactory("t1"));
    assertNotNull(javaTableConfig.getTableProviderFactory("t2"));
  }

  public static class MockLocalTableDescriptor<K, V> extends LocalTableDescriptor<K, V, MockLocalTableDescriptor<K, V>> {

    public MockLocalTableDescriptor(String tableId, KVSerde<K, V> serde) {
      super(tableId, serde);
    }

    public String getProviderFactoryClassName() {
      return "some.class";
    }
  }

}
