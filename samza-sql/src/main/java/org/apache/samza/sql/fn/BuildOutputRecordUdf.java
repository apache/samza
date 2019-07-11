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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.Validate;
import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.udfs.SamzaSqlUdf;
import org.apache.samza.sql.udfs.SamzaSqlUdfMethod;
import org.apache.samza.sql.udfs.ScalarUdf;


/**
 * BuildOutputRecordUdf builds a SamzaSqlRelRecord with given list of key value pairs.
 * Useful if you need to populate fields for a Kafka message.
 *
 * For example, given args = {k1, v1, k2, v2},
 * it returns a SamzaSqlRelRecord with fieldNames={k1, k2} and fieldValues={v1, v2},
 * where v1 or v2 can be any Object, including SamzaSqlRelRecord when you want to set nested field.
 *
 * Consider the below nested output schema:
 * {
 *   field1,
 *   field2:{
 *     field21,
 *     field22
 *   },
 *   field3:{
 *     field31:{
 *       field311,
 *       field312
 *     },
 *     field32
 *   }
 * };
 * It could be built in the select statement as:
 * select obj1 as field1,
 *        BuildSamzaSqlRelRecord("field21", obj21, "field22", obj22) as field2,
 *        BuildSamzaSqlRelRecord("field31", BuildSamzaSqlRelRecord("field311", obj311, "field312", obj312),
 *                               "field32", obj32) as field3
 *
 * If no args is provided, it returns an empty SamzaSqlRelRecord (with empty field names and values list).
 */

@SamzaSqlUdf(name = "BuildOutputRecord", description = "Creates an Output record.")
public class BuildOutputRecordUdf implements ScalarUdf {
  @Override
  public void init(Config udfConfig, Context context) {
  }

  @SamzaSqlUdfMethod(disableArgumentCheck = true)
  public SamzaSqlRelRecord execute(Object... args) {
    int numOfArgs = args.length;
    Validate.isTrue(numOfArgs % 2 == 0, "numOfArgs should be an even number");

    List<String> fieldNames = new ArrayList<>();
    List<Object> fieldValues = new ArrayList<>();

    for (int i = 0; i < numOfArgs - 1; i += 2) {
      fieldNames.add((String) args[i]);
      // value can be instanceof SamzaSqlRelRecord, or any Object(string, int, long most likely)
      fieldValues.add(args[i + 1]);
    }

    return new SamzaSqlRelRecord(fieldNames, fieldValues);
  }
}
