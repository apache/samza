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

package org.apache.samza.sql.dsl;

import java.util.Collection;
import java.util.Collections;
import org.apache.calcite.rel.RelRoot;
import org.apache.samza.sql.interfaces.DslConverter;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;


public class SamzaSqlDslConverter implements DslConverter {

  private SamzaSqlApplicationConfig sqlConfig;

  SamzaSqlDslConverter(SamzaSqlApplicationConfig sqlConfig) {
    this.sqlConfig = sqlConfig;
  }

  @Override
  public Collection<RelRoot> convertDsl(String dsl) {
    QueryPlanner planner =
        new QueryPlanner(sqlConfig.getRelSchemaProviders(), sqlConfig.getSystemStreamConfigsBySource(),
            sqlConfig.getUdfMetadata());
    final RelRoot relRoot = planner.plan(dsl);
    return Collections.singletonList(relRoot);
  }
}
