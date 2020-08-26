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

import java.util.Objects;
import java.util.Map;

/**
 * A model to represent the locality information of an application. The locality information refers to the
 * whereabouts of the physical execution of a samza container. With this information, samza achieves (best effort) affinity
 * i.e. place the container on the host in which it was running before. By doing this, stateful applications can minimize
 * the bootstrap time of their state by leveraging the local copy.
 *
 * It is suffice to have only {@link ProcessorLocality} model and use it within locality manager. However, this abstraction
 * enables us extend locality beyond container. e.g. It is useful to track task locality to enable heterogeneous containers
 * or fine grained execution model.
 *
 * In YARN deployment model, processors are interchangeably used for container and <i>processorId</i>refers to
 * logical container id.
 */
public class LocalityModel {
  /*
   * A collection of processor locality keyed by processorId.
   */
  private Map<String, ProcessorLocality> processorLocalities;

  /**
   * Construct locality model for the job from the input map of processor localities.
   * @param processorLocalities host locality information for the job keyed by processor id
   */
  public LocalityModel(Map<String, ProcessorLocality> processorLocalities) {
    this.processorLocalities = processorLocalities;
  }

  /*
   * Returns a {@link Map} of {@link ProcessorLocality} keyed by processors id.
   */
  public Map<String, ProcessorLocality> getProcessorLocalities() {
    return processorLocalities;
  }

  /*
   * Returns the {@link ProcessorLocality} for the given container processorId.
   */
  public ProcessorLocality getProcessorLocality(String processorId) {
    return processorLocalities.get(processorId);
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
    return Objects.deepEquals(processorLocalities, that.processorLocalities);
  }

  @Override
  public int hashCode() {
    return Objects.hash(processorLocalities);
  }
}
