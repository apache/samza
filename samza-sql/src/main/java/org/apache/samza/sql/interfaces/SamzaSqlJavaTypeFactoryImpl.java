/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.samza.sql.interfaces;

import com.google.common.collect.Lists;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.type.JavaToSqlTypeConversionRules;
import org.apache.calcite.sql.type.SqlTypeName;


/**
 * Calcite does validation of projected field types in select statement with the output schema types. If one of the
 * projected fields is an UDF with return type of {@link Object} or any other java type not defined in
 * {@link JavaToSqlTypeConversionRules}, using the default {@link JavaTypeFactoryImpl} results in validation failure.
 * Hence, extending {@link JavaTypeFactoryImpl} to make Calcite validation work with all output types of Samza SQL UDFs.
 */
public class SamzaSqlJavaTypeFactoryImpl
    extends JavaTypeFactoryImpl {

  public SamzaSqlJavaTypeFactoryImpl() {
    this(RelDataTypeSystem.DEFAULT);
  }

  public SamzaSqlJavaTypeFactoryImpl(RelDataTypeSystem typeSystem) {
    super(typeSystem);
  }

  @Override
  public RelDataType toSql(RelDataType type) {
    return toSql(this, type);
  }

  /** Converts a type in Java format to a SQL-oriented type. */
  public static RelDataType toSql(final RelDataTypeFactory typeFactory,
      RelDataType type) {
    if (type instanceof RelRecordType) {
      return typeFactory.createStructType(
          Lists.transform(type.getFieldList(), a0 -> toSql(typeFactory, a0.getType())),
          type.getFieldNames());
    }
    if (type instanceof JavaType) {
      SqlTypeName typeName = JavaToSqlTypeConversionRules.instance().lookup(((JavaType) type).getJavaClass());
      // For unknown sql type names, return ANY sql type to make Calcite validation not fail.
      if (typeName == null) {
        typeName = SqlTypeName.ANY;
      }
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(typeName),
          type.isNullable());
    } else {
      return JavaTypeFactoryImpl.toSql(typeFactory, type);
    }
  }
}
