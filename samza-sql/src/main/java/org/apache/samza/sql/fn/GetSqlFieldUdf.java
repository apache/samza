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

package org.apache.samza.sql.fn;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang.Validate;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.udfs.SamzaSqlUdf;
import org.apache.samza.sql.udfs.SamzaSqlUdfMethod;
import org.apache.samza.sql.udfs.ScalarUdf;


/**
 * UDF that extracts a field value from a nested SamzaSqlRelRecord by recursively following a query path.
 * Note that the root object must be a SamzaSqlRelRecord.
 *
 * Syntax for field specification:
 * <ul>
 *  <li> SamzaSqlRelRecord/Map: <code> field.subfield </code> </li>
 *  <li> Array: <code> field[index] </code> </li>
 *  <li> Scalar types: <code> field </code> </li>
 * </ul>
 *
 * Example query: <code> pageViewEvent.requestHeader.properties.cookies[3].sessionKey </code>
 *
 * Above query extracts the sessionKey field from below nested record:
 *
 *   pageViewEvent (SamzaSqlRelRecord)
 *     - requestHeader (SamzaSqlRelRecord)
 *       - properties (Map)
 *         - cookies (Array)
 *           - sessionKey (Scalar)
 *
 */
@SamzaSqlUdf(name = "GetSqlField", description = "Get an element from complex Sql field as a String.")
public class GetSqlFieldUdf implements ScalarUdf {
  @Override
  public void init(Config udfConfig, Context context) {
  }

  @SamzaSqlUdfMethod(params = {SamzaSqlFieldType.ANY, SamzaSqlFieldType.STRING})
  public String execute(Object field, String fieldName) {
    Object currentFieldOrValue = field;
    Validate.isTrue(currentFieldOrValue == null
        || currentFieldOrValue instanceof SamzaSqlRelRecord);

    String[] fieldNameChain = fieldName.split("\\.");
    for (int i = 0; i < fieldNameChain.length && currentFieldOrValue != null; i++) {
      currentFieldOrValue = extractField(fieldNameChain[i], currentFieldOrValue);
    }

    if (currentFieldOrValue != null) {
      return currentFieldOrValue.toString();
    }

    return null;
  }

  static Object extractField(String fieldName, Object current) {
    if (current instanceof SamzaSqlRelRecord) {
      SamzaSqlRelRecord record = (SamzaSqlRelRecord) current;
      Validate.isTrue(record.getFieldNames().contains(fieldName),
          String.format("Invalid field %s in %s", fieldName, record));
      return record.getField(fieldName).orElse(null);
    } else if (current instanceof Map) {
      Map map = (Map) current;
      Validate.isTrue(map.containsKey(fieldName), String.format("Invalid field %s in %s", fieldName, map));
      return map.get(fieldName);
    } else if (current instanceof List && fieldName.endsWith("]")) {
      List list = (List) current;
      int index = Integer.parseInt(fieldName.substring(fieldName.indexOf("[") + 1, fieldName.length() - 1));
      return list.get(index);
    }

    throw new IllegalArgumentException(String.format(
        "Unsupported accessing operation for data type: %s with field: %s.", current.getClass(), fieldName));
  }
}
