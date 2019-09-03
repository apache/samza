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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.dsl.SamzaSqlDslConverter;
import org.apache.samza.sql.interfaces.RelSchemaProvider;
import org.apache.samza.sql.interfaces.SamzaSqlJavaTypeFactoryImpl;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.util.SamzaSqlQueryParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SamzaSqlValidator that uses calcite engine to convert the sql query to relational graph and validates the query
 * including the output.
 */
public class SamzaSqlValidator {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaSqlValidator.class);

  private final Config config;

  public SamzaSqlValidator(Config config) {
    this.config = config;
  }

  /**
   * Validate a list of sql statements
   * @param sqlStmts list of sql statements
   * @throws SamzaSqlValidatorException
   */
  public void validate(List<String> sqlStmts) throws SamzaSqlValidatorException {
    SamzaSqlApplicationConfig sqlConfig = SamzaSqlDslConverter.getSqlConfig(sqlStmts, config);
    QueryPlanner planner = SamzaSqlDslConverter.getQueryPlanner(sqlConfig);

    for (String sql: sqlStmts) {
      // we always pass only select query to the planner for samza sql. The reason is that samza sql supports
      // schema evolution where source and destination could up to an extent have independent schema evolution while
      // calcite expects strict conformance of the destination schema with that of the fields in the select query.
      SamzaSqlQueryParser.QueryInfo qinfo = SamzaSqlQueryParser.parseQuery(sql);
      RelRoot relRoot;
      try {
        relRoot = planner.plan(qinfo.getSelectQuery());
      } catch (SamzaException e) {
        throw new SamzaSqlValidatorException("Calcite planning for sql failed.", e);
      }

      // Now that we have logical plan, validate different aspects.
      validate(relRoot, qinfo, sqlConfig);
    }
  }

  protected void validate(RelRoot relRoot, SamzaSqlQueryParser.QueryInfo qinfo, SamzaSqlApplicationConfig sqlConfig)
      throws SamzaSqlValidatorException {
    // Validate select fields (including Udf return types) with output schema
    validateOutput(relRoot, sqlConfig.getRelSchemaProviders().get(qinfo.getSink()));

    // TODO:
    //  1. Validate Udf arguments.
    //  2. Validate operators. These are the operators that are supported by Calcite but not by Samza Sql.
    //     Eg: LogicalAggregate is not supported by Samza Sql.
  }

  protected void validateOutput(RelRoot relRoot, RelSchemaProvider relSchemaProvider) throws SamzaSqlValidatorException {
    RelRecordType outputRecord = (RelRecordType) QueryPlanner.getSourceRelSchema(relSchemaProvider,
        new RelSchemaConverter());
    LogicalProject project = (LogicalProject) relRoot.rel;
    RelRecordType projetRecord = (RelRecordType) project.getRowType();
    validateOutputRecords(outputRecord, projetRecord);
  }

  protected void validateOutputRecords(RelRecordType outputRecord, RelRecordType projectRecord)
      throws SamzaSqlValidatorException {
    Map<String, RelDataType> outputRecordMap = outputRecord.getFieldList().stream().collect(
        Collectors.toMap(RelDataTypeField::getName, RelDataTypeField::getType));
    Map<String, RelDataType> projectRecordMap = projectRecord.getFieldList().stream().collect(
        Collectors.toMap(RelDataTypeField::getName, RelDataTypeField::getType));

    // There could be default values for the output schema and hence fields in project schema could be a subset of
    // fields in output schema.
    // TODO: Validate that all non-default value fields in output schema are set in the projected fields.
    for (Map.Entry<String, RelDataType> entry : projectRecordMap.entrySet()) {
      RelDataType outputFieldType = outputRecordMap.get(entry.getKey());
      if (outputFieldType == null) {
        if (entry.getKey().equals(SamzaSqlRelMessage.OP_NAME)) {
          continue;
        }
        String errMsg = String.format("Field '%s' in select query does not match any field in output schema.",
            entry.getKey());
        LOG.error(errMsg);
        throw new SamzaSqlValidatorException(errMsg);
      } else if (!compareFieldTypes(outputFieldType, entry.getValue())) {
        String errMsg = String.format("Field '%s' with type '%s' in select query does not match the field type '%s' in"
            + " output schema.", entry.getKey(), entry.getValue(), outputFieldType);
        LOG.error(errMsg);
        throw new SamzaSqlValidatorException(errMsg);
      }
    }
  }

  protected boolean compareFieldTypes(RelDataType outputFieldType, RelDataType selectQueryFieldType) {
    RelDataType projectFieldType;

    // JavaTypes are relevant for Udf argument and return types
    // TODO: Support UDF argument validation. Currently, only return types are validated and argument types are
    //  validated during run-time.
    if (selectQueryFieldType instanceof RelDataTypeFactoryImpl.JavaType) {
      projectFieldType = new SamzaSqlJavaTypeFactoryImpl().toSql(selectQueryFieldType);
    } else {
      projectFieldType = selectQueryFieldType;
    }

    SqlTypeName outputSqlType = outputFieldType.getSqlTypeName();
    SqlTypeName projectSqlType = projectFieldType.getSqlTypeName();

    if (projectSqlType == SqlTypeName.ANY || outputSqlType == SqlTypeName.ANY) {
      return true;
    } else if (outputSqlType != SqlTypeName.ROW && outputSqlType == projectSqlType) {
      return true;
    }

    switch (outputSqlType) {
      case CHAR:
        return projectSqlType == SqlTypeName.VARCHAR;
      case VARCHAR:
        return projectSqlType == SqlTypeName.CHAR;
      case BIGINT:
        return projectSqlType == SqlTypeName.INTEGER;
      case INTEGER:
        return projectSqlType == SqlTypeName.BIGINT;
      case FLOAT:
        return projectSqlType == SqlTypeName.DOUBLE;
      case DOUBLE:
        return projectSqlType == SqlTypeName.FLOAT;
      case ROW:
        try {
          validateOutputRecords((RelRecordType) outputFieldType, (RelRecordType) projectFieldType);
        } catch (SamzaSqlValidatorException e) {
          LOG.error("A field in select query does not match with the output schema.", e);
          return false;
        }
        return true;
      default:
          return false;
    }
  }

  // -- All Static Methods below --

  /**
   * Format the Calcite exception to a more readable form.
   *
   * As an example, consider the below sql query which fails calcite validation due to a non existing field :
   * "Insert into testavro.outputTopic(id) select non_existing_name, name as string_value"
   *             + " from testavro.level1.level2.SIMPLE1 as s where s.id = 1"
   *
   * This function takes in the above multi-line sql query and the below sample exception as input:
   * "org.apache.calcite.runtime.CalciteContextException: From line 1, column 8 to line 1, column 26: Column
   * 'non_existing_name' not found in any table"
   *
   * And returns the following string:
   * 2019-08-30 09:05:08 ERROR QueryPlanner:174 - Failed with exception for the following sql statement:
   *
   * Sql syntax error:
   *
   * SELECT `non_existing_name`, `name` AS `string_value`
   * -------^^^^^^^^^^^^^^^^^^^--------------------------
   * FROM `testavro`.`level1`.`level2`.`SIMPLE1` AS `s`
   * WHERE `s`.`id` = 1
   *
   * @param query sql query
   * @param e Exception returned by Calcite
   * @return formatted error string
   */
  public static String formatErrorString(String query, Exception e) {
    Pattern pattern = Pattern.compile("line [0-9]+, column [0-9]+");
    Matcher matcher = pattern.matcher(e.getMessage());
    String[] queryLines = query.split("\\n");
    StringBuilder result = new StringBuilder();
    int startColIdx, endColIdx, startLineIdx, endLineIdx;

    try {
      if (matcher.find()) {
        String match = matcher.group();
        LOG.info(match);
        startLineIdx = getIdxFromString(match, "line ");
        startColIdx = getIdxFromString(match, "column ");
        if (matcher.find()) {
          match = matcher.group();
          LOG.info(match);
          endLineIdx = getIdxFromString(match, "line ");
          endColIdx = getIdxFromString(match, "column ");
        } else {
          endColIdx = startColIdx;
          endLineIdx = startLineIdx;
        }
        int lineLen = endLineIdx - startLineIdx;
        int colLen = endColIdx - startColIdx + 1;

        // Error spanning across multiple lines is not supported yet.
        if (lineLen > 0) {
          throw new SamzaException("lineLen formatting validation error: error cannot span across multiple lines.");
        }

        int lineIdx = 0;
        for (String line : queryLines) {
          result.append(line)
              .append("\n");
          if (lineIdx == startLineIdx) {
            String lineStr = getStringWithRepeatedChars('-', line.length() - 1);
            String pointerStr = getStringWithRepeatedChars('^', colLen);
            String errorMarkerStr =
                new StringBuilder(lineStr).replace(startColIdx, endColIdx, pointerStr).toString();
            result.append(errorMarkerStr)
                .append("\n");
          }
          lineIdx++;
        }
      }

      String[] errorMsgParts = e.getMessage().split("Exception:");
      result.append("\n")
          .append(errorMsgParts[errorMsgParts.length - 1].trim());
      return String.format("Sql syntax error:\n\n%s\n",
          result);
    } catch (Exception ex) {
      // Ignore any formatting errors.
      LOG.error("Formatting error (Not the actual error. Look for the logs for actual error)", ex);
      return String.format("Failed with formatting exception (not the actual error) for the following sql"
              + " statement:\n\"%s\"\n\n%s", query, e.getMessage());
    }
  }

  private static int getIdxFromString(String inputString, String delimiterStr) {
    String[] splitStr = inputString.split(delimiterStr);
    Scanner in = new Scanner(splitStr[1]).useDelimiter("[^0-9]+");
    return in.nextInt() - 1;
  }

  private static String getStringWithRepeatedChars(char ch, int len) {
    char[] chars = new char[len];
    Arrays.fill(chars, ch);
    return new String(chars);
  }
}
