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

package org.apache.samza.sql.util;

import org.apache.samza.config.Config;
import org.apache.samza.context.Context;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.udfs.SamzaSqlUdf;
import org.apache.samza.sql.udfs.SamzaSqlUdfMethod;
import org.apache.samza.sql.udfs.ScalarUdf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test UDF used by unit and integration tests.
 */
@SamzaSqlUdf(name = "MyTest", description = "Test UDF.")
public class MyTestUdf implements ScalarUdf {

  private static final Logger LOG = LoggerFactory.getLogger(MyTestUdf.class);

  @SamzaSqlUdfMethod(params = SamzaSqlFieldType.INT32)
  public Integer execute(Integer value) {
    return value * 2;
  }

  @SamzaSqlUdfMethod(params = SamzaSqlFieldType.ANY)
  public Integer execute(Object value) {
    return ((Integer) value) * 2;
  }


  @Override
  public void init(Config udfConfig, Context context) {
    LOG.info("Init called with {}", udfConfig);
  }
}


