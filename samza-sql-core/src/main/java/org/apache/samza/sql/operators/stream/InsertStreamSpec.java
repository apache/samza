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

package org.apache.samza.sql.operators.stream;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.operators.spec.OperatorSpec;
import org.apache.samza.sql.operators.factory.SimpleOperatorSpec;


/**
 * Example implementation of specification of <code>InsertStream</code> operator
 */
public class InsertStreamSpec extends SimpleOperatorSpec implements OperatorSpec {

  /**
   * Default ctor of <code>InsertStreamSpec</code>
   *
   * @param id The identifier of the operator
   * @param input The input relation entity
   * @param output The output stream entity
   */
  public InsertStreamSpec(String id, EntityName input, EntityName output) {
    super(id, input, output);
  }
}
