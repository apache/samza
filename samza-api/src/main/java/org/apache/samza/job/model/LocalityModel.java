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

package org.apache.samza.job.model;

import com.google.common.base.Objects;
import java.util.Map;

/**
 * A model to represent the locality mapping of an application.
 * Currently the locality mapping represents the container host locality of an application.
 *
 * We want to keep the locality mapping open and not tie it to a container to potentially
 * support heterogeneous container where in task locality (unit of work within samza) will be useful
 * to track.
 */
public class LocalityModel {
  private Map<String, HostLocality> hostLocalities;

  /**
   * Construct locality model for the job from the input map of container localities.
   * @param hostLocalities host locality information for the job keyed by container id
   */
  public LocalityModel(Map<String, HostLocality> hostLocalities) {
    this.hostLocalities = hostLocalities;
  }

  public Map<String, HostLocality> getHostLocalities() {
    return hostLocalities;
  }

  public HostLocality getHostLocality(String id) {
    return hostLocalities.get(id);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LocalityModel)) {
      return false;
    }
    LocalityModel that = (LocalityModel) o;
    return Objects.equal(hostLocalities, that.hostLocalities);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(hostLocalities);
  }
}
