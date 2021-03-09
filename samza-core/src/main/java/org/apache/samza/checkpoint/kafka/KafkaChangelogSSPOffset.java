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
package org.apache.samza.checkpoint.kafka;

import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.checkpoint.CheckpointId;

/**
 * Used in {@link org.apache.samza.checkpoint.CheckpointV1} for tracking the latest offset for store changelogs at
 * the time of commit. Checkpointed changelog offset has the format: [checkpointId, offset], separated by a colon.
 */
@InterfaceStability.Unstable
public class KafkaChangelogSSPOffset {
  public static final String SEPARATOR = ":";

  private final CheckpointId checkpointId;
  private final String changelogOffset;

  public KafkaChangelogSSPOffset(CheckpointId checkpointId, String changelogOffset) {
    this.checkpointId = checkpointId;
    this.changelogOffset = changelogOffset;
  }

  public static KafkaChangelogSSPOffset fromString(String message) {
    if (StringUtils.isBlank(message)) {
      throw new IllegalArgumentException("Invalid checkpointed changelog message: " + message);
    }
    String[] checkpointIdAndOffset = message.split(SEPARATOR);
    if (checkpointIdAndOffset.length != 2) {
      throw new IllegalArgumentException("Invalid checkpointed changelog offset: " + message);
    }
    CheckpointId checkpointId = CheckpointId.fromString(checkpointIdAndOffset[0]);
    String offset = null;
    if (!"null".equals(checkpointIdAndOffset[1])) {
      offset = checkpointIdAndOffset[1];
    }

    return new KafkaChangelogSSPOffset(checkpointId, offset);
  }

  public CheckpointId getCheckpointId() {
    return checkpointId;
  }

  public String getChangelogOffset() {
    return changelogOffset;
  }

  /**
   * WARNING: Do not change the toString() representation. It is used for serde'ing the store changelog offsets
   * as part of task checkpoints, in conjunction with {@link #fromString(String)}.
   * @return the String representation of this {@link KafkaChangelogSSPOffset}
   */
  @Override
  public String toString() {
    return String.format("%s%s%s", checkpointId, SEPARATOR, changelogOffset);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    KafkaChangelogSSPOffset that = (KafkaChangelogSSPOffset) o;
    return Objects.equals(checkpointId, that.checkpointId) &&
        Objects.equals(changelogOffset, that.changelogOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(checkpointId, changelogOffset);
  }
}
