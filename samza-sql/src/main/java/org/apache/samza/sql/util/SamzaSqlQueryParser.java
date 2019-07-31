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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.interfaces.SamzaSqlDriver;
import org.apache.samza.sql.interfaces.SamzaSqlJavaTypeFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class that is used to parse the Samza sql query to figure out the sources, sink etc..
 */
public class SamzaSqlQueryParser {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaSqlQueryParser.class);

  private SamzaSqlQueryParser() {
  }

  public static class QueryInfo {
    private final List<String> sources;
    private String selectQuery;
    private String sink;
    private String sql;

    public QueryInfo(String selectQuery, List<String> sources, String sink, String sql) {
      this.selectQuery = selectQuery;
      this.sink = sink;
      this.sources = sources;
      this.sql = sql;
    }

    public List<String> getSources() {
      return sources;
    }

    public String getSelectQuery() {
      return selectQuery;
    }

    public String getSink() {
      return sink;
    }

    public String getSql() {
      return sql;
    }
  }

  public static QueryInfo parseQuery(String sql) {
    Planner planner = createPlanner();
    SqlNode sqlNode;
    try {
      sqlNode = planner.parse(sql);
    } catch (SqlParseException e) {
      throw new SamzaException(e);
    }

    String sink;
    String selectQuery;
    ArrayList<String> sources;
    if (sqlNode instanceof SqlInsert) {
      SqlInsert sqlInsert = ((SqlInsert) sqlNode);
      sink = sqlInsert.getTargetTable().toString();
      if (sqlInsert.getSource() instanceof SqlSelect) {
        SqlSelect sqlSelect = (SqlSelect) sqlInsert.getSource();
        selectQuery = sqlSelect.toString();
        LOG.info("Parsed select query {} from sql {}", selectQuery, sql);
        sources = getSourcesFromSelectQuery(sqlSelect);
      } else {
        String msg = String.format("Sql query is not of the expected format. Select node expected, found %s",
            sqlInsert.getSource().getClass().toString());
        LOG.error(msg);
        throw new SamzaException(msg);
      }
    } else {
      String msg = String.format("Sql query is not of the expected format. Insert node expected, found %s",
          sqlNode.getClass().toString());
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    return new QueryInfo(selectQuery, sources, sink, sql);
  }

  private static Planner createPlanner() {
    Connection connection;
    SchemaPlus rootSchema;
    try {
      JavaTypeFactory typeFactory = new SamzaSqlJavaTypeFactoryImpl();
      SamzaSqlDriver driver = new SamzaSqlDriver(typeFactory);
      DriverManager.deregisterDriver(DriverManager.getDriver("jdbc:calcite:"));
      DriverManager.registerDriver(driver);
      connection = driver.connect("jdbc:calcite:", new Properties());
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      rootSchema = calciteConnection.getRootSchema();
    } catch (SQLException e) {
      throw new SamzaException(e);
    }

    final List<RelTraitDef> traitDefs = new ArrayList<>();

    traitDefs.add(ConventionTraitDef.INSTANCE);
    traitDefs.add(RelCollationTraitDef.INSTANCE);

    FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
        .parserConfig(SqlParser.configBuilder().setLex(Lex.JAVA).build())
        .defaultSchema(rootSchema)
        .operatorTable(SqlStdOperatorTable.instance())
        .traitDefs(traitDefs)
        .context(Contexts.EMPTY_CONTEXT)
        .costFactory(null)
        .build();
    return Frameworks.getPlanner(frameworkConfig);
  }

  private static ArrayList<String> getSourcesFromSelectQuery(SqlSelect sqlSelect) {
    ArrayList<String> sources = new ArrayList<>();
    getSource(sqlSelect.getFrom(), sources);
    if (sources.size() < 1) {
      throw new SamzaException("Unsupported query " + sqlSelect);
    }

    return sources;
  }

  private static void getSource(SqlNode node, ArrayList<String> sourceList) {
    if (node instanceof SqlJoin) {
      SqlJoin joinNode = (SqlJoin) node;
      ArrayList<String> sourcesLeft = new ArrayList<>();
      ArrayList<String> sourcesRight = new ArrayList<>();
      getSource(joinNode.getLeft(), sourcesLeft);
      getSource(joinNode.getRight(), sourcesRight);

      sourceList.addAll(sourcesLeft);
      sourceList.addAll(sourcesRight);
    } else if (node instanceof SqlIdentifier) {
      sourceList.add(node.toString());
    } else if (node instanceof SqlBasicCall) {
      SqlBasicCall basicCall = ((SqlBasicCall) node);
      if (basicCall.getOperator() instanceof SqlAsOperator) {
        getSource(basicCall.operand(0), sourceList);
      } else if (basicCall.getOperator() instanceof SqlUnnestOperator && basicCall.operand(0) instanceof SqlSelect) {
        sourceList.addAll(getSourcesFromSelectQuery(basicCall.operand(0)));
      }
    } else if (node instanceof SqlSelect) {
      getSource(((SqlSelect) node).getFrom(), sourceList);
    }
  }
}
