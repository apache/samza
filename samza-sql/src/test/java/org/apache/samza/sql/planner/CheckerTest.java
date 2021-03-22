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
import java.lang.reflect.Method;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.Context;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.udfs.SamzaSqlUdf;
import org.apache.samza.sql.udfs.SamzaSqlUdfMethod;
import org.apache.samza.sql.udfs.ScalarUdf;
import org.apache.samza.sql.util.MyTestPolyUdf;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

public class CheckerTest {

  @SamzaSqlUdf(name = "TestUdfWithWrongTypes", description = "TestUDFClass")
  private static class TestUdfWithWrongTypes implements ScalarUdf {

    public TestUdfWithWrongTypes() {
    }

    @Override
    public void init(Config udfConfig, Context context) {
    }

    @SamzaSqlUdfMethod(params = SamzaSqlFieldType.INT32, returns = SamzaSqlFieldType.INT64)
    public String execute(String val) {
      return "RandomStringtoFail";
    }
  }

  @SamzaSqlUdf(name = "TestUdfWithAnyType", description = "TestUDFClass")
  private static class TestUdfWithAnyType implements ScalarUdf {

    public TestUdfWithAnyType() {
    }

    @Override
    public void init(Config udfConfig, Context context) {
    }

    @SamzaSqlUdfMethod(params = SamzaSqlFieldType.ANY, returns = SamzaSqlFieldType.STRING)
    public String execute(Object val) {
      return "RandomStringtoFail";
    }
  }

  @Test(expected = SamzaSqlValidatorException.class)
  public void testCheckOperandTypesShouldFailOnTypeMisMatch() throws NoSuchMethodException {
    Method udfMethod = TestUdfWithWrongTypes.class.getMethod("execute", String.class);
    UdfMetadata udfMetadata = new UdfMetadata("TestUdfWithWrongTypes", "TestUDFClass",
            udfMethod, new MapConfig(), ImmutableList.of(SamzaSqlFieldType.INT32), SamzaSqlFieldType.INT64, false);

    Checker operandTypeChecker = Checker.getChecker(1, 3, udfMetadata);

    SqlCallBinding callBinding = Mockito.mock(SqlCallBinding.class);
    Mockito.when(callBinding.getOperandCount()).thenReturn(1);
    Mockito.when(callBinding.getOperandType(0)).thenReturn(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR, 12));

    operandTypeChecker.checkOperandTypes(callBinding, true);
  }

  @Test
  public void testCheckOperandTypesShouldReturnTrueOnTypeMatch() throws NoSuchMethodException {
    Method udfMethod = MyTestPolyUdf.class.getMethod("execute", String.class);
    UdfMetadata udfMetadata = new UdfMetadata("MyTestPoly", "Test Polymorphism UDF.",
        udfMethod, new MapConfig(), ImmutableList.of(SamzaSqlFieldType.STRING), SamzaSqlFieldType.INT32, false);

    Checker operandTypeChecker = Checker.getChecker(1, 3, udfMetadata);

    SqlCallBinding callBinding = Mockito.mock(SqlCallBinding.class);
    Mockito.when(callBinding.getOperandCount()).thenReturn(1);
    Mockito.when(callBinding.getOperandType(0)).thenReturn(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR, 12));

    assertTrue(operandTypeChecker.checkOperandTypes(callBinding, true));
  }

  @Test
  public void testCheckOperandTypesShouldReturnTrueOnAnyTypeInArg() throws NoSuchMethodException {
    Method udfMethod = TestUdfWithAnyType.class.getMethod("execute", Object.class);
    UdfMetadata udfMetadata = new UdfMetadata("TestUdfWithAnyType", "TestUDFClass",
        udfMethod, new MapConfig(), ImmutableList.of(SamzaSqlFieldType.ANY), SamzaSqlFieldType.INT64, false);

    Checker operandTypeChecker = Checker.getChecker(1, 3, udfMetadata);

    SqlCallBinding callBinding = Mockito.mock(SqlCallBinding.class);
    Mockito.when(callBinding.getOperandCount()).thenReturn(1);
    Mockito.when(callBinding.getOperandType(0)).thenReturn(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.ARRAY));

    assertTrue(operandTypeChecker.checkOperandTypes(callBinding, true));
  }

  @Test
  public void testCheckOperandTypesShouldReturnTrueWhenArgumentCheckIsDisabled() throws NoSuchMethodException {
    Method udfMethod = TestUdfWithWrongTypes.class.getMethod("execute", String.class);
    UdfMetadata udfMetadata = new UdfMetadata("TestUdfWithWrongTypes", "TestUDFClass",
            udfMethod, new MapConfig(), ImmutableList.of(SamzaSqlFieldType.INT32), SamzaSqlFieldType.INT64, true);

    Checker operandTypeChecker = Checker.getChecker(1, 3, udfMetadata);

    SqlCallBinding callBinding = Mockito.mock(SqlCallBinding.class);
    Mockito.when(callBinding.getOperandCount()).thenReturn(1);
    Mockito.when(callBinding.getOperandType(0)).thenReturn(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR, 12));

    assertTrue(operandTypeChecker.checkOperandTypes(callBinding, true));
  }

  @Test
  public void testCheckOperandTypesShouldReturnFalseWhenThrowOnFailureIsFalse() throws NoSuchMethodException {
    Method udfMethod = MyTestPolyUdf.class.getMethod("execute", String.class);
    UdfMetadata udfMetadata = new UdfMetadata("MyTestPoly", "Test Polymorphism UDF.",
            udfMethod, new MapConfig(), ImmutableList.of(SamzaSqlFieldType.STRING), SamzaSqlFieldType.INT32, false);

    Checker operandTypeChecker = Checker.getChecker(1, 3, udfMetadata);

    SqlCallBinding callBinding = Mockito.mock(SqlCallBinding.class);
    Mockito.when(callBinding.getOperandCount()).thenReturn(1);
    Mockito.when(callBinding.getOperandType(0)).thenReturn(new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR, 12));

    assertTrue(operandTypeChecker.checkOperandTypes(callBinding, false));
  }
}