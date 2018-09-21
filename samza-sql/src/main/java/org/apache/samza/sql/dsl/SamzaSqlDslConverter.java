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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelRoot;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.interfaces.DslConverter;
import org.apache.samza.sql.planner.QueryPlanner;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.sql.testutil.SqlFileParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.sql.runner.SamzaSqlApplicationConfig.*;


public class SamzaSqlDslConverter implements DslConverter {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaSqlDslConverter.class);

  private final Config config;

  SamzaSqlDslConverter(Config config) {
    this.config = config;
  }

  @Override
  public Collection<RelRoot> convertDsl(String dsl) {
    // TODO: Introduce an API to parse a dsl string and return one or more sql statements
    List<String> sqlStmts = fetchSqlFromConfig(config);
    List<SamzaSqlQueryParser.QueryInfo> queryInfo = fetchQueryInfo(sqlStmts);
    SamzaSqlApplicationConfig sqlConfig = new SamzaSqlApplicationConfig(config,
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSources).flatMap(Collection::stream)
            .collect(Collectors.toSet()),
        queryInfo.stream().map(SamzaSqlQueryParser.QueryInfo::getSink).collect(Collectors.toSet()));

    QueryPlanner planner =
        new QueryPlanner(sqlConfig.getRelSchemaProviders(), sqlConfig.getSystemStreamConfigsBySource(),
            sqlConfig.getUdfMetadata());

    List<RelRoot> relRoots = new LinkedList<>();
    for (String sql: sqlStmts) {
      relRoots.add(planner.plan(sql));
    }
    return relRoots;
  }

  public static List<SamzaSqlQueryParser.QueryInfo> fetchQueryInfo(List<String> sqlStmts) {
    return sqlStmts.stream().map(SamzaSqlQueryParser::parseQuery).collect(Collectors.toList());
  }

  public static List<String> fetchSqlFromConfig(Map<String, String> config) {
    List<String> sql;
    if (config.containsKey(CFG_SQL_STMT) && StringUtils.isNotBlank(config.get(CFG_SQL_STMT))) {
      String sqlValue = config.get(CFG_SQL_STMT);
      sql = Collections.singletonList(sqlValue);
    } else if (config.containsKey(CFG_SQL_STMTS_JSON) && StringUtils.isNotBlank(config.get(CFG_SQL_STMTS_JSON))) {
      sql = deserializeSqlStmts(config.get(CFG_SQL_STMTS_JSON));
    } else if (config.containsKey(CFG_SQL_FILE)) {
      String sqlFile = config.get(CFG_SQL_FILE);
      sql = SqlFileParser.parseSqlFile(sqlFile);
    } else {
      String msg = "Config doesn't contain the SQL that needs to be executed.";
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    return sql;
  }
}
