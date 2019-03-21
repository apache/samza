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
 *//*
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
 *//*
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
package org.apache.samza.startpoint;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.SerializerProvider;


final class StartpointKeySerializer extends JsonSerializer<StartpointKey> {
  @Override
  public void serialize(StartpointKey startpointKey, JsonGenerator jsonGenerator, SerializerProvider provider) throws
                                                                                                               IOException {
    Map<String, Object> systemStreamPartitionMap = new HashMap<>();
    SystemStreamPartition systemStreamPartition = startpointKey.getSystemStreamPartition();
    TaskName taskName = startpointKey.getTaskName();
    systemStreamPartitionMap.put("system", systemStreamPartition.getSystem());
    systemStreamPartitionMap.put("stream", systemStreamPartition.getStream());
    systemStreamPartitionMap.put("partition", systemStreamPartition.getPartition().getPartitionId());
    if (taskName != null) {
      systemStreamPartitionMap.put("taskName", taskName.getTaskName());
    }
    jsonGenerator.writeObject(systemStreamPartitionMap);
  }
}