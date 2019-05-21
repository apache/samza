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

package org.apache.samza.context;

import org.apache.samza.job.model.JobModel;
import org.apache.samza.system.StreamMetadataCache;

import java.util.HashMap;
import java.util.Map;

public class InternalTaskContext {

  private final Context context;
  private final Map<String, Object> objectRegistry = new HashMap<>();

  public InternalTaskContext(Context context) {
    this.context = context;
  }

  public void registerObject(String name, Object value) {
    this.objectRegistry.put(name, value);
  }

  public Object fetchObject(String name) {
    return this.objectRegistry.get(name);
  }

  public Context getContext() {
    return context;
  }

  public JobModel getJobModel() {
    return ((TaskContextImpl) this.context.getTaskContext()).getJobModel();
  }

  public StreamMetadataCache getStreamMetadataCache() {
    return ((TaskContextImpl) this.context.getTaskContext()).getStreamMetadataCache();
  }
}
