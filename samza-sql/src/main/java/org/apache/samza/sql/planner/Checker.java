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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.SamzaException;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.udfs.SamzaSqlUdfMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Checker implements SqlOperandTypeChecker {

  private static final Logger LOG = LoggerFactory.getLogger(Checker.class);
  private static final List<SqlTypeName> ANY_SQL_TYPE_NAMES = ImmutableList.of(SqlTypeName.ANY, SqlTypeName.OTHER);
  static final Checker ANY_CHECKER = new Checker();

  private final Optional<UdfMetadata> udfMetadataOptional;
  private final SqlOperandCountRange range;

  public static Checker getChecker(int min, int max, UdfMetadata udfMetadata) {
    if (min == max) {
      return new Checker(min, udfMetadata);
    } else {
      return new Checker(min, max, udfMetadata);
    }
  }

  private Checker(int size, UdfMetadata udfMetadata) {
    this.range = SqlOperandCountRanges.of(size);
    this.udfMetadataOptional = Optional.of(udfMetadata);
  }

  private Checker(int min, int max, UdfMetadata udfMetadata) {
    this.range = SqlOperandCountRanges.between(min, max);
    this.udfMetadataOptional = Optional.of(udfMetadata);
  }

  private Checker() {
    this.range = SqlOperandCountRanges.any();
    this.udfMetadataOptional = Optional.empty();
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    if (!udfMetadataOptional.isPresent() || udfMetadataOptional.get().isDisableArgCheck() || !throwOnFailure) {
      return true;
    } else {
      // 1. Generate a mapping from argument index to parsed calcite-type for the sql UDF.
      Map<Integer, RelDataType> argumentIndexToCalciteType = IntStream.range(0, callBinding.getOperandCount())
          .boxed()
          .collect(Collectors.toMap(operandIndex -> operandIndex, callBinding::getOperandType, (a, b) -> b));

      UdfMetadata udfMetadata = udfMetadataOptional.get();
      List<SamzaSqlFieldType> udfArguments = udfMetadata.getArguments();

      // 2. Compare the argument type in samza-sql UDF against the RelType generated by the
      // calcite parser engine.
      for (int udfArgumentIndex = 0; udfArgumentIndex < udfArguments.size(); ++udfArgumentIndex) {
        SamzaSqlFieldType udfArgumentType = udfArguments.get(udfArgumentIndex);
        SqlTypeName udfArgumentAsSqlType = toCalciteSqlType(udfArgumentType);
        RelDataType parsedSqlArgType = argumentIndexToCalciteType.get(udfArgumentIndex);

        // 3(a). Special-case, where static strings used as method-arguments in udf-methods during invocation are parsed as the Char type by calcite.
        if (parsedSqlArgType.getSqlTypeName() == SqlTypeName.CHAR && udfArgumentAsSqlType == SqlTypeName.VARCHAR) {
          return true;
        } else if (!Objects.equals(parsedSqlArgType.getSqlTypeName(), udfArgumentAsSqlType)
            && !ANY_SQL_TYPE_NAMES.contains(parsedSqlArgType.getSqlTypeName()) && hasOneUdfMethod(udfMetadata)) {
          // 3(b). Throw up and fail on mismatch between the SamzaSqlType and CalciteType for any argument.
          String msg = String.format(
              "Type mismatch in udf class: %s at argument index: %d." + "Expected type: %s, actual type: %s.",
              udfMetadata.getName(), udfArgumentIndex, udfArgumentAsSqlType, parsedSqlArgType.getSqlTypeName());
          LOG.error(msg);
          throw new SamzaSqlValidatorException(msg);
        }
      }
    }
    // 4. The SamzaSqlFieldType and CalciteType has matched for all the arguments in the UDF.
    return true;
  }

  /**
   * Checks if there is only one UdfMethod in the input {@link UdfMetadata}.
   * @param udfMetadata the metadata for a UDF.
   * @return true if there is only one udf method defined in the UdfMetadata.
   *         false otherwise.
   */
  @VisibleForTesting
  boolean hasOneUdfMethod(UdfMetadata udfMetadata) {
    Class<?> udfClass = udfMetadata.getUdfMethod().getDeclaringClass();
    int numAnnotatedUdfMethods = 0;
    for (Method method : udfClass.getMethods()) {
      if (method.isAnnotationPresent(SamzaSqlUdfMethod.class)) {
        numAnnotatedUdfMethods += 1;
      }
    }
    return numAnnotatedUdfMethods == 1;
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return range;
  }

  @Override
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return opName + "(Drill - Opaque)";
  }

  @Override
  public Consistency getConsistency() {
    return Consistency.NONE;
  }

  @Override
  public boolean isOptional(int i) {
    return false;
  }

  /**
   * Converts the {@link SamzaSqlFieldType} to the calcite {@link SqlTypeName}.
   * @param samzaSqlFieldType the samza sql field type.
   * @return the converted calcite SqlTypeName.
   */
  @VisibleForTesting
  static SqlTypeName toCalciteSqlType(SamzaSqlFieldType samzaSqlFieldType) {
    switch (samzaSqlFieldType) {
      case ANY:
        return SqlTypeName.ANY;
      case ROW:
        return SqlTypeName.ROW;
      case MAP:
        return SqlTypeName.MAP;
      case ARRAY:
        return SqlTypeName.ARRAY;
      case REAL:
        return SqlTypeName.REAL;
      case DOUBLE:
        return SqlTypeName.DOUBLE;
      case STRING:
        return SqlTypeName.VARCHAR;
      case INT16:
      case INT32:
        return SqlTypeName.INTEGER;
      case FLOAT:
        return SqlTypeName.FLOAT;
      case INT64:
        return SqlTypeName.BIGINT;
      case BOOLEAN:
        return SqlTypeName.BOOLEAN;
      case BYTES:
        return SqlTypeName.VARBINARY;
      default:
        String msg = String.format("Field Type %s is not supported", samzaSqlFieldType);
        LOG.error(msg);
        throw new SamzaException(msg);
    }
  }
}