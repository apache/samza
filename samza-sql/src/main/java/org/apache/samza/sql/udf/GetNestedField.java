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

package org.apache.samza.sql.udf;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.EnumUtils;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

import static org.apache.calcite.schema.impl.ReflectiveFunctionBase.builder;


/**
 * Operator to extract nested Rows or Fields form a struct row type using a dotted path.
 * The goal of this operator is two-fold.
 * First it is a temporary fix for https://issues.apache.org/jira/browse/CALCITE-4065 to extract a row from a row.
 * Second it will enable smooth backward compatible migration from existing udf that relies on legacy row format.
 */
public class GetNestedField extends SqlUserDefinedFunction {

  public static final SqlFunction INSTANCE = new GetNestedField(new ExtractFunction());

  public GetNestedField(Function function) {
    super(new SqlIdentifier("GetNestedField", SqlParserPos.ZERO), null, null, null, ImmutableList.of(), function);
  }

  @Override
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  @Override
  public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    final SqlNode left = callBinding.operand(0);
    final SqlNode right = callBinding.operand(1);
    final RelDataType type = callBinding.getValidator().deriveType(callBinding.getScope(), left);
    boolean isRow = true;
    if (type.getSqlTypeName() != SqlTypeName.ROW) {
      isRow = false;
    } else if (type.getSqlIdentifier().isStar()) {
      isRow = false;
    }
    if (!isRow && throwOnFailure) {
      throw callBinding.newValidationSignatureError();
    }
    return isRow && OperandTypes.STRING.checkSingleOperandType(callBinding, right, 0, throwOnFailure);
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType recordType = opBinding.getOperandType(0);
    switch (recordType.getSqlTypeName()) {
      case ROW:
        final String fieldName = opBinding.getOperandLiteralValue(1, String.class);
        String[] fieldNameChain = fieldName.split("\\.");
        RelDataType relDataType = opBinding.getOperandType(0);
        for (int i = 0; i < fieldNameChain.length; i++) {
          RelDataTypeField t = relDataType.getField(fieldNameChain[i], true, true);
          Preconditions.checkNotNull(t,
              "Can not find " + fieldNameChain[i] + " within record " + recordType.toString() + " Original String "
                  + Arrays.toString(fieldNameChain) + " Original row " + recordType.toString());
          relDataType = t.getType();
        }
        if (recordType.isNullable()) {
          return typeFactory.createTypeWithNullability(relDataType, true);
        } else {
          return relDataType;
        }
      default:
        throw new AssertionError("First Operand is suppose to be a Row Struct");
    }
  }

  private static class ExtractFunction implements ScalarFunction, ImplementableFunction {
    private final JavaTypeFactoryImpl javaTypeFactory = new JavaTypeFactoryImpl();

    @Override
    public CallImplementor getImplementor() {
      return RexImpTable.createImplementor((translator, call, translatedOperands) -> {
        Preconditions.checkState(translatedOperands.size() == 2 && call.operands.size() == 2,
            "Expected 2 operands found " + Math.min(translatedOperands.size(), call.getOperands().size()));
        Expression op0 = translatedOperands.get(0);
        Expression op1 = translatedOperands.get(1);
        Preconditions.checkState(op1.getNodeType().equals(ExpressionType.Constant),
            "Operand 2 has to be constant and got " + op1.getNodeType());
        Preconditions.checkState(op1.type.equals(String.class), "Operand 2 has to be String and got " + op1.type);
        final String fieldName = (String) ((ConstantExpression) op1).value;
        String[] fieldNameChain = fieldName.split("\\.");
        RelDataType relDataType = call.operands.get(0).getType();
        Preconditions.checkState(relDataType.getSqlTypeName().equals(SqlTypeName.ROW),
            "Expected first operand to be ROW found " + relDataType.toString());
        Expression currentExpression = op0;
        for (int i = 0; i < fieldNameChain.length; i++) {
          Preconditions.checkState(relDataType.getSqlTypeName() == SqlTypeName.ROW,
              "Must be ROW found " + relDataType.toString());
          RelDataTypeField t = relDataType.getField(fieldNameChain[i], true, true);
          Preconditions.checkNotNull(t,
              "Notfound " + fieldNameChain[i] + " in the following struct " + relDataType.toString()
                  + " Original String " + Arrays.toString(fieldNameChain) + " Original row " + call.operands.get(0)
                  .getType());
          currentExpression = Expressions.arrayIndex(Expressions.convert_(currentExpression, Object[].class),
              Expressions.constant(t.getIndex()));
          relDataType = t.getType();
        }
        Type fieldType = javaTypeFactory.getJavaClass(relDataType);
        return EnumUtils.convert(currentExpression, fieldType);
      }, NullPolicy.ARG0, false);
    }

    @Override
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
      throw new IllegalStateException("should not be called");
    }

    @Override
    public List<FunctionParameter> getParameters() {
      return builder().add(Object[].class, "row").add(String.class, "path").build();
    }
  }

  @Override
  public String getAllowedSignatures(String opNameToUse) {
    return opNameToUse + "(<ROW>, <VARCHAR>)";
  }
}
