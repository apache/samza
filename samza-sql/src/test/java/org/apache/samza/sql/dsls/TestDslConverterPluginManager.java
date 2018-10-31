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

package org.apache.samza.sql.dsls;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.rel.RelRoot;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.interfaces.DslConverter;
import org.apache.samza.sql.interfaces.DslConverterFactory;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.junit.Assert;
import org.junit.Test;


public class TestDslConverterPluginManager {
  static class TestDslConverterFactory implements DslConverterFactory {
    public DslConverter create(Config config) {
      return new TestDslConverter();
    }
  }

  static class TestDslConverter implements DslConverter {
    public Collection<RelRoot> convertDsl(String dslStmts) {
      return Collections.emptyList();
    }
  }

  @Test
  public void testDslConverterCreation() {
    Map<String, String> config = new HashMap<>();
    config.put(SamzaSqlApplicationConfig.CFG_SQL_FILE, "samza.test");
    config.put(String.format(SamzaSqlApplicationConfig.CFG_DSL_FORMAT_FACTORY, "test"),
        TestDslConverterFactory.class.getName());
    DslConverter dslConverter = DslConverterPluginManager.create(new MapConfig(config));
    Assert.assertTrue(dslConverter instanceof TestDslConverter);
  }
}
