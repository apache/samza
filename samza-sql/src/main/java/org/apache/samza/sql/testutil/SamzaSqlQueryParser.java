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

package org.apache.samza.sql.testutil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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


/**
 * Utility class that is used to parse the Samza sql query to figure out the sources, sink etc..
 */
public class SamzaSqlQueryParser {

  private SamzaSqlQueryParser() {
  }

  public static class QueryInfo {
    private final List<String> sources;
    private String selectQuery;
    private String sink;

    public QueryInfo(String selectQuery, List<String> sources, String sink) {
      this.selectQuery = selectQuery;
      this.sink = sink;
      this.sources = sources;
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
  }

  public static QueryInfo parseQuery(String sql) {

    Pattern insertIntoSqlPattern = Pattern.compile("insert into (.*) (select .* from (.*))", Pattern.CASE_INSENSITIVE);
    Matcher m = insertIntoSqlPattern.matcher(sql);
    if (!m.matches()) {
      throw new SamzaException("Invalid query format");
    }

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
        selectQuery = m.group(2);
        sources = getSourcesFromSelectQuery(sqlSelect);
      } else {
        throw new SamzaException("Sql query is not of the expected format");
      }
    } else {
      throw new SamzaException("Sql query is not of the expected format");
    }

    return new QueryInfo(selectQuery, sources, sink);
  }

  private static Planner createPlanner() {
    Connection connection;
    SchemaPlus rootSchema;
    try {
      connection = DriverManager.getConnection("jdbc:calcite:");
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
        return;
      }
    } else if (node instanceof SqlSelect) {
      getSource(((SqlSelect) node).getFrom(), sourceList);
    }
  }
}
