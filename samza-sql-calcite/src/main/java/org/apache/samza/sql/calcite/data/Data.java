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

package org.apache.samza.sql.calcite.data;

import java.util.List;
import java.util.Map;


/**
 * A generic data interface that allows to implement data access / deserialization w/ {@link Schema}
 */
public interface Data {

  Schema schema();

  Object value();

  int intValue();

  long longValue();

  float floatValue();

  double doubleValue();

  boolean booleanValue();

  String strValue();

  byte[] bytesValue();

  List<Object> arrayValue();

  Map<Object, Object> mapValue();

  Data getElement(int index);

  Data getFieldData(String fldName);

}
