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

import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.udfs.SamzaSqlUdf;
import org.apache.samza.sql.udfs.SamzaSqlUdfMethod;
import org.apache.samza.sql.udfs.ScalarUdf;


@SamzaSqlUdf(name = "GetNestedField", description = "UDF that extracts a field value from a nested SamzaSqlRelRecord")
public class GetNestedFieldUdf implements ScalarUdf {
  @Override
  public void init(Config udfConfig, Context context) {
  }

  @SamzaSqlUdfMethod(params = {SamzaSqlFieldType.ANY, SamzaSqlFieldType.STRING},
      returns = SamzaSqlFieldType.ANY)
  public Object execute(Object currentFieldOrValue, String fieldName) {
    GetSqlFieldUdf udf = new GetSqlFieldUdf();
    return udf.getSqlField(currentFieldOrValue, fieldName);
  }
}
