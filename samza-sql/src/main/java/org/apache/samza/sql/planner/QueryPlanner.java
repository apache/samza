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

package org.apache.samza.sql.planner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.RelSchemaProvider;
import org.apache.samza.sql.interfaces.SqlSystemStreamConfig;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * QueryPlanner that uses calcite engine to convert the sql query to relational graph.
 */
public class QueryPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(QueryPlanner.class);

  private final Map<String, SqlSystemStreamConfig> systemStreamConfigBySource;
  private final Collection<UdfMetadata> udfMetadata;
  private final Map<SystemStream, RelSchemaProvider> relSchemaProviders;

  public QueryPlanner(Map<SystemStream, RelSchemaProvider> relSchemaProviders,
      Map<String, SqlSystemStreamConfig> systemStreamConfigBySource, Collection<UdfMetadata> udfMetadata) {
    this.relSchemaProviders = relSchemaProviders;
    this.systemStreamConfigBySource = systemStreamConfigBySource;
    this.udfMetadata = udfMetadata;
  }

  public RelRoot plan(String query) {
    try {
      Connection connection = DriverManager.getConnection("jdbc:calcite:");
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      Map<String, SchemaPlus> systemSchemas = new HashMap<>();

      for (SqlSystemStreamConfig ssc : systemStreamConfigBySource.values()) {

        RelSchemaProvider relSchemaProvider = relSchemaProviders.get(ssc.getSystemStream());
        SchemaPlus systemSchema =
            systemSchemas.computeIfAbsent(ssc.getSystemName(), s -> rootSchema.add(s, new AbstractSchema()));
        RelDataType relationalSchema = relSchemaProvider.getRelationalSchema();

        systemSchema.add(ssc.getStreamName(), createTableFromRelSchema(relationalSchema));
      }

      List<SamzaSqlScalarFunctionImpl> samzaSqlFunctions = udfMetadata.stream()
          .map(x -> new SamzaSqlScalarFunctionImpl(x.getName(), x.getUdfMethod()))
          .collect(Collectors.toList());

      final List<RelTraitDef> traitDefs = new ArrayList<>();

      traitDefs.add(ConventionTraitDef.INSTANCE);
      traitDefs.add(RelCollationTraitDef.INSTANCE);

      List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
      sqlOperatorTables.add(new SamzaSqlOperatorTable());
      sqlOperatorTables.add(new SamzaSqlUdfOperatorTable(samzaSqlFunctions));

      FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
          .parserConfig(SqlParser.configBuilder().setLex(Lex.JAVA).build())
          .defaultSchema(rootSchema)
          .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables))
          .traitDefs(traitDefs)
          .context(Contexts.EMPTY_CONTEXT)
          .costFactory(null)
          .build();
      Planner planner = Frameworks.getPlanner(frameworkConfig);

      SqlNode sql = planner.parse(query);
      SqlNode validatedSql = planner.validate(sql);
      RelRoot relRoot = planner.rel(validatedSql);
      LOG.info("query plan:\n" + sql.toString());
      LOG.info("relational graph:");
      printRelGraph(relRoot.project());
      return relRoot;
    } catch (Exception e) {
      LOG.error("Query planner failed with exception.", e);
      throw new SamzaException(e);
    }
  }

  private Table createTableFromRelSchema(RelDataType relationalSchema) {
    return new AbstractTable() {
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<RelDataTypeField> fieldsList = new ArrayList<>();
        fieldsList.add(new RelDataTypeFieldImpl(SamzaSqlRelMessage.KEY_NAME, 0,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true)));
        fieldsList.addAll(relationalSchema.getFieldList());
        return new RelRecordType(fieldsList);
      }
    };
  }

  private static void printRelGraph(RelNode node) {
    RelJsonWriter jsonWriter = new RelJsonWriter();
    node.explain(jsonWriter);
    LOG.info(jsonWriter.asString());
  }
}
