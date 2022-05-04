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
package org.apache.samza.elasticity.util;

/**
 * POJO class to store system, stream, partition, and keyBucket information associated with a Task,
 *  that is encoded in the task's name.
 */
public class TaskNameComponents {

  public static final int DEFAULT_KEY_BUCKET = 0;
  public static final int DEFAULT_ELASTICITY_FACTOR = 1;
  public static final int INVALID_PARTITION = -1;

  public final String system;
  public final String stream;
  public final int partition;
  public final int keyBucket;
  public final int elasticityFactor;

  public TaskNameComponents(int partition) {
    this(partition, DEFAULT_KEY_BUCKET, DEFAULT_ELASTICITY_FACTOR);
  }

  public TaskNameComponents(int partition, int keyBucket, int elasticityFactor) {
    this("", "", partition, keyBucket, elasticityFactor);
  }

  public TaskNameComponents(String system, String stream, int partition) {
    this(system, stream, partition, DEFAULT_KEY_BUCKET, DEFAULT_ELASTICITY_FACTOR);
  }

  public TaskNameComponents(String system, String stream, int partition, int keyBucket, int elasticityFactor) {
    this.system = system;
    this.stream = stream;
    this.partition = partition;
    this.keyBucket = keyBucket;
    this.elasticityFactor = elasticityFactor;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TaskNameComponents)) return false;

    TaskNameComponents that = (TaskNameComponents) o;

    if (!(this.system.equals(that.system))
        || !(this.stream.equals(that.stream))
        || (this.partition != that.partition)
        || (this.keyBucket != that.keyBucket)
        || (this.elasticityFactor != that.elasticityFactor)) {
      return false;
    }
    return true;
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + system.hashCode();
    result = prime * result + stream.hashCode();
    result = prime * result + partition;
    result = prime * result + keyBucket;
    result = prime * result + elasticityFactor;
    return result;
  }
}
