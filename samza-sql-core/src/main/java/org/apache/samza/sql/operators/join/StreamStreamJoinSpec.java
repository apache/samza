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

package org.apache.samza.sql.operators.join;

import java.util.ArrayList;
import java.util.List;

import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.operators.SimpleOperatorSpec;


/**
 * This class defines the specification of a {@link org.apache.samza.sql.operators.join.StreamStreamJoin} operator
 */
public class StreamStreamJoinSpec extends SimpleOperatorSpec {

  public StreamStreamJoinSpec(String id, List<EntityName> inputs, EntityName output, List<String> joinKeys) {
    super(id, inputs, output);
    // TODO Auto-generated constructor stub
  }

  @SuppressWarnings("serial")
  public StreamStreamJoinSpec(String id, List<String> inputRelations, String output, List<String> joinKeys) {
    super(id, new ArrayList<EntityName>() {
      {
        for (String input : inputRelations) {
          add(EntityName.getStreamName(input));
        }
      }
    }, EntityName.getStreamName(output));
    // TODO Auto-generated constructor stub
  }

}
