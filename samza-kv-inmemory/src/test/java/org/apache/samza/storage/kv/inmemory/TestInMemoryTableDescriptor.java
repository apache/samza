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

package org.apache.samza.storage.kv.inmemory;

import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.table.TableSpec;
import org.junit.Assert;
import org.junit.Test;


public class TestInMemoryTableDescriptor {
  @Test
  public void testTableSpec() {

    TableSpec tableSpec = new InMemoryTableDescriptor<Integer, String>("1")
        .withSerde(KVSerde.of(new IntegerSerde(), new StringSerde()))
        .withConfig("inmemory.abc", "xyz")
        .getTableSpec();

    Assert.assertNotNull(tableSpec.getSerde());
    Assert.assertNotNull(tableSpec.getSerde().getKeySerde());
    Assert.assertNotNull(tableSpec.getSerde().getValueSerde());
    Assert.assertEquals("xyz", getConfig(tableSpec, "abc"));
  }

  private String getConfig(TableSpec tableSpec, String key) {
    return tableSpec.getConfig().get("inmemory." + key);
  }
}
