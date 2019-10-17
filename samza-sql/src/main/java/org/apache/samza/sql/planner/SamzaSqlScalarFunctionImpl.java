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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.apache.samza.sql.udfs.ScalarUdf;

/**
 * Calcite implementation for Samza SQL UDF.
 * This class contains logic to generate the java code to execute {@link org.apache.samza.sql.udfs.SamzaSqlUdf}.
 */
public class SamzaSqlScalarFunctionImpl implements ScalarFunction, ImplementableFunction {

  private final ScalarFunction myIncFunction;
  private final Method udfMethod;
  private final Method getUdfMethod;
  private final String udfName;
  private final UdfMetadata udfMetadata;

  public SamzaSqlScalarFunctionImpl(UdfMetadata udfMetadata) {

    myIncFunction = ScalarFunctionImpl.create(udfMetadata.getUdfMethod());
    this.udfMetadata = udfMetadata;
    this.udfName = udfMetadata.getName();
    this.udfMethod = udfMetadata.getUdfMethod();
    this.getUdfMethod = Arrays.stream(SamzaSqlExecutionContext.class.getMethods())
        .filter(x -> x.getName().equals("getOrCreateUdf"))
        .findFirst()
        .get();
  }

  public String getUdfName() {
    return udfName;
  }

  public int numberOfArguments() {
    return udfMetadata.getArguments().size();
  }

  public UdfMetadata getUdfMetadata() {
    return udfMetadata;
  }

  @Override
  public CallImplementor getImplementor() {
    return RexImpTable.createImplementor((translator, call, translatedOperands) -> {
      final Expression sqlContext = Expressions.parameter(SamzaSqlExecutionContext.class, "sqlContext");
      final Expression samzaContext = Expressions.parameter(SamzaSqlExecutionContext.class, "context");
      final Expression getUdfInstance = Expressions.call(ScalarUdf.class, sqlContext, getUdfMethod,
          Expressions.constant(udfMethod.getDeclaringClass().getName()), Expressions.constant(udfName), samzaContext);

      List<Expression> convertedOperands = new ArrayList<>();
      // SAMZA: 2230 To allow UDFS to accept Untyped arguments.
      // We explicitly Convert the untyped arguments to type that the UDf expects.
      for(int index = 0; index < translatedOperands.size(); index++) {
        if (!udfMetadata.isDisableArgCheck() && translatedOperands.get(index).type == Object.class
            && udfMethod.getParameters()[index].getType() != Object.class) {
          convertedOperands.add(Expressions.convert_(translatedOperands.get(index), udfMethod.getParameters()[index].getType()));
        } else {
          convertedOperands.add(translatedOperands.get(index));
        }
      }

      final Expression callExpression = Expressions.call(Expressions.convert_(getUdfInstance, udfMethod.getDeclaringClass()), udfMethod,
          convertedOperands);
      return callExpression;
    }, NullPolicy.NONE, false);
  }

  @Override
  public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
    return myIncFunction.getReturnType(typeFactory);
  }

  @Override
  public List<FunctionParameter> getParameters() {
    return myIncFunction.getParameters();
  }
}
