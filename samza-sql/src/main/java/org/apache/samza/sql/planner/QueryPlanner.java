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

import com.google.common.collect.ImmutableList;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Programs;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.RelSchemaProvider;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.schema.SqlFieldSchema;
import org.apache.samza.sql.schema.SqlSchema;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * QueryPlanner that uses calcite engine to convert the sql query to relational graph.
 */
public class QueryPlanner {
  private static final Logger LOG = LoggerFactory.getLogger(QueryPlanner.class);

  private final Collection<UdfMetadata> udfMetadata;

  // Mapping between the source to the RelSchemaProvider corresponding to the source.
  private final Map<String, RelSchemaProvider> relSchemaProviders;

  // Mapping between the source to the SqlIOConfig corresponding to the source.
  private final Map<String, SqlIOConfig> systemStreamConfigBySource;

  private final boolean isQueryPlanOptimizerEnabled;

  public QueryPlanner(Map<String, RelSchemaProvider> relSchemaProviders,
      Map<String, SqlIOConfig> systemStreamConfigBySource, Collection<UdfMetadata> udfMetadata,
      boolean isQueryPlanOptimizerEnabled) {
    this.relSchemaProviders = relSchemaProviders;
    this.systemStreamConfigBySource = systemStreamConfigBySource;
    this.udfMetadata = udfMetadata;
    this.isQueryPlanOptimizerEnabled = isQueryPlanOptimizerEnabled;
  }

  private void registerSourceSchemas(SchemaPlus rootSchema) {
    RelSchemaConverter relSchemaConverter = new RelSchemaConverter();

    for (SqlIOConfig ssc : systemStreamConfigBySource.values()) {
      SchemaPlus previousLevelSchema = rootSchema;
      List<String> sourceParts = ssc.getSourceParts();
      RelSchemaProvider relSchemaProvider = relSchemaProviders.get(ssc.getSource());

      for (int sourcePartIndex = 0; sourcePartIndex < sourceParts.size(); sourcePartIndex++) {
        String sourcePart = sourceParts.get(sourcePartIndex);
        if (sourcePartIndex < sourceParts.size() - 1) {
          SchemaPlus sourcePartSchema = previousLevelSchema.getSubSchema(sourcePart);
          if (sourcePartSchema == null) {
            sourcePartSchema = previousLevelSchema.add(sourcePart, new AbstractSchema());
          }
          previousLevelSchema = sourcePartSchema;
        } else {
          // If the source part is the last one, then fetch the schema corresponding to the stream and register.
          RelDataType relationalSchema = getSourceRelSchema(relSchemaProvider, relSchemaConverter);
          previousLevelSchema.add(sourcePart, createTableFromRelSchema(relationalSchema));
          break;
        }
      }
    }
  }

  private Planner getPlanner() {
    Planner planner = null;
    try {
      Connection connection = DriverManager.getConnection("jdbc:calcite:");
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      registerSourceSchemas(rootSchema);

      List<SamzaSqlScalarFunctionImpl> samzaSqlFunctions = udfMetadata.stream()
          .map(SamzaSqlScalarFunctionImpl::new)
          .collect(Collectors.toList());

      final List<RelTraitDef> traitDefs = new ArrayList<>();

      traitDefs.add(ConventionTraitDef.INSTANCE);
      traitDefs.add(RelCollationTraitDef.INSTANCE);

      List<SqlOperatorTable> sqlOperatorTables = new ArrayList<>();
      sqlOperatorTables.add(new SamzaSqlOperatorTable());
      sqlOperatorTables.add(new SamzaSqlUdfOperatorTable(samzaSqlFunctions));

      // TODO: Introduce a pluggable rule factory.
      List<RelOptRule> rules = ImmutableList.of(
          FilterProjectTransposeRule.INSTANCE,
          ProjectMergeRule.INSTANCE,
          new SamzaSqlFilterRemoteJoinRule.SamzaSqlFilterIntoRemoteJoinRule(true, RelFactories.LOGICAL_BUILDER,
          systemStreamConfigBySource));

      // Using lenient so that !=,%,- are allowed.
      FrameworkConfig frameworkConfig = Frameworks.newConfigBuilder()
          .parserConfig(SqlParser.configBuilder()
              .setLex(Lex.JAVA)
              .setConformance(SqlConformanceEnum.LENIENT)
              .setCaseSensitive(false) // Make Udfs case insensitive
              .build())
          .defaultSchema(rootSchema)
          .operatorTable(new ChainedSqlOperatorTable(sqlOperatorTables))
          .sqlToRelConverterConfig(SqlToRelConverter.Config.DEFAULT)
          .traitDefs(traitDefs)
          .programs(Programs.hep(rules, true, DefaultRelMetadataProvider.INSTANCE))
          .build();
      planner = Frameworks.getPlanner(frameworkConfig);
      return planner;
    } catch (Exception e) {
      String errorMsg = "Failed to create planner.";
      LOG.error(errorMsg, e);
      if (planner != null) {
        planner.close();
      }
      throw new SamzaException(errorMsg, e);
    }
  }

  private RelRoot optimize(Planner planner, RelRoot relRoot) {
    RelTraitSet relTraitSet = RelTraitSet.createEmpty();
    try {
      RelRoot optimizedRelRoot =
          RelRoot.of(planner.transform(0, relTraitSet, relRoot.project()), SqlKind.SELECT);
      LOG.info("query plan with optimization:\n"
          + RelOptUtil.toString(optimizedRelRoot.rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
      return optimizedRelRoot;
    } catch (Exception e) {
      String errorMsg =
          "Error while optimizing query plan:\n" + RelOptUtil.toString(relRoot.rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
      LOG.error(errorMsg, e);
      throw new SamzaException(errorMsg, e);
    }
  }

  public RelRoot plan(String query) {
    try (Planner planner = getPlanner()) {
      SqlNode sql = planner.parse(query);
      SqlNode validatedSql = planner.validate(sql);
      RelRoot relRoot = planner.rel(validatedSql);
      LOG.info(
          "query plan without optimization:\n" + RelOptUtil.toString(relRoot.rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES));
      if (!isQueryPlanOptimizerEnabled) {
        LOG.info("Skipping query optimization as it is disabled.");
        return relRoot;
      }
      return optimize(planner, relRoot);
    } catch (Exception e) {
      String errorMsg = SamzaSqlValidator.formatErrorString(query, e);
      LOG.error(errorMsg, e);
      throw new SamzaException(errorMsg, e);
    }
  }

  public static SqlSchema getSourceSqlSchema(RelSchemaProvider relSchemaProvider) {
    SqlSchema sqlSchema = relSchemaProvider.getSqlSchema();

    List<String> fieldNames = new ArrayList<>();
    List<SqlFieldSchema> fieldTypes = new ArrayList<>();
    if (!sqlSchema.containsField(SamzaSqlRelMessage.KEY_NAME)) {
      fieldNames.add(SamzaSqlRelMessage.KEY_NAME);
      // Key is a nullable and optional field. It is defaulted to null in SamzaSqlRelMessage.
      fieldTypes.add(SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.ANY, true, true));
    }

    fieldNames.addAll(
        sqlSchema.getFields().stream().map(SqlSchema.SqlField::getFieldName).collect(Collectors.toList()));
    fieldTypes.addAll(
        sqlSchema.getFields().stream().map(SqlSchema.SqlField::getFieldSchema).collect(Collectors.toList()));

    return new SqlSchema(fieldNames, fieldTypes);
  }

  public static RelDataType getSourceRelSchema(RelSchemaProvider relSchemaProvider,
      RelSchemaConverter relSchemaConverter) {
    // If the source part is the last one, then fetch the schema corresponding to the stream and register.
    return relSchemaConverter.convertToRelSchema(getSourceSqlSchema(relSchemaProvider));
  }

  private static Table createTableFromRelSchema(RelDataType relationalSchema) {
    return new AbstractTable() {
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return relationalSchema;
      }
    };
  }
}
