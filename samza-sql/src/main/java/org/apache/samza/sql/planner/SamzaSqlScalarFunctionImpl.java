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
import org.apache.samza.sql.udfs.ScalarUdf;


public class SamzaSqlScalarFunctionImpl implements ScalarFunction, ImplementableFunction {

  private final ScalarFunction myIncFunction;
  private final Method udfMethod;
  private final Method getUdfMethod;


  private final String udfName;

  public SamzaSqlScalarFunctionImpl(String udfName, Method udfMethod) {
    myIncFunction = ScalarFunctionImpl.create(udfMethod);
    this.udfName = udfName;
    this.udfMethod = udfMethod;
    this.getUdfMethod = Arrays.stream(SamzaSqlExecutionContext.class.getMethods())
        .filter(x -> x.getName().equals("getOrCreateUdf"))
        .findFirst()
        .get();
  }

  public String getUdfName() {
    return udfName;
  }

  @Override
  public CallImplementor getImplementor() {
    return RexImpTable.createImplementor((translator, call, translatedOperands) -> {
      final Expression context = Expressions.parameter(SamzaSqlExecutionContext.class, "context");
      final Expression getUdfInstance = Expressions.call(ScalarUdf.class, context, getUdfMethod,
          Expressions.constant(udfMethod.getDeclaringClass().getName()), Expressions.constant(udfName));
      final Expression callExpression = Expressions.convert_(Expressions.call(Expressions.convert_(getUdfInstance, udfMethod.getDeclaringClass()), udfMethod,
          translatedOperands), Object.class);
      // The Janino compiler which is used to compile the expressions doesn't seem to understand the Type of the ScalarUdf.execute
      // because it is a generic. To work around that we are explicitly casting it to the return type.
      return Expressions.convert_(callExpression, udfMethod.getReturnType());
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
