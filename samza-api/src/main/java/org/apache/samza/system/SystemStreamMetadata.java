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

package org.apache.samza.system;

import java.util.Collections;
import java.util.Map;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;

/**
 * SystemAdmins use this class to return useful metadata about a stream's offset
 * and partition information.
 */
public class SystemStreamMetadata {
  private final String streamName;
  private final Map<Partition, SystemStreamPartitionMetadata> partitionMetadata;

  public SystemStreamMetadata(String streamName, Map<Partition, SystemStreamPartitionMetadata> partitionMetadata) {
    this.streamName = streamName;
    this.partitionMetadata = partitionMetadata;
  }

  /**
   * @return The stream name that's associated with the metadata contained in an
   *         instance of this class.
   */
  public String getStreamName() {
    return streamName;
  }

  /**
   * @return A map of SystemStreamPartitionMetadata that includes offset
   *         information for each partition.
   */
  public Map<Partition, SystemStreamPartitionMetadata> getSystemStreamPartitionMetadata() {
    return Collections.unmodifiableMap(partitionMetadata);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((partitionMetadata == null) ? 0 : partitionMetadata.hashCode());
    result = prime * result + ((streamName == null) ? 0 : streamName.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    SystemStreamMetadata other = (SystemStreamMetadata) obj;
    if (partitionMetadata == null) {
      if (other.partitionMetadata != null)
        return false;
    } else if (!partitionMetadata.equals(other.partitionMetadata))
      return false;
    if (streamName == null) {
      if (other.streamName != null)
        return false;
    } else if (!streamName.equals(other.streamName))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "SystemStreamMetadata [streamName=" + streamName + ", partitionMetadata=" + partitionMetadata + "]";
  }

  /**
   * Provides offset information for a given SystemStreamPartition. This
   * currently only includes offset information.
   */
  public static class SystemStreamPartitionMetadata {
    private final String oldestOffset;
    private final String newestOffset;
    private final String upcomingOffset;

    public SystemStreamPartitionMetadata(String oldestOffset, String newestOffset, String upcomingOffset) {
      this.oldestOffset = oldestOffset;
      this.newestOffset = newestOffset;
      this.upcomingOffset = upcomingOffset;
    }

    /**
     * @return The oldest offset that still exists in the stream for the
     *         partition given. If a partition has two messages with offsets 0
     *         and 1, respectively, then this method would return 0 for the
     *         oldest offset. This offset is useful when one wishes to read all
     *         messages in a stream from the very beginning. A null value means
     *         the stream is empty.
     */
    public String getOldestOffset() {
      return oldestOffset;
    }

    /**
     * @return The newest offset that exists in the stream for the partition
     *         given. If a partition has two messages with offsets 0 and 1,
     *         respectively, then this method would return 1 for the newest
     *         offset. This offset is useful when one wishes to see if all
     *         messages have been read from a stream (offset of last message
     *         read == newest offset). A null value means the stream is empty.
     */
    public String getNewestOffset() {
      return newestOffset;
    }

    /**
     * @return The offset that represents the next message to be written in the
     *         stream for the partition given. If a partition has two messages
     *         with offsets 0 and 1, respectively, then this method would return
     *         2 for the upcoming offset. This offset is useful when one wishes
     *         to pick up reading at the very end of a stream. A null value
     *         means the stream is empty.
     */
    public String getUpcomingOffset() {
      return upcomingOffset;
    }

    /**
     * @param offsetType
     *          The type of offset to get. Either oldest, newest, or upcoming.
     * @return The corresponding offset for the offset type requested.
     */
    public String getOffset(OffsetType offsetType) {
      if (offsetType.equals(OffsetType.OLDEST)) {
        return getOldestOffset();
      } else if (offsetType.equals(OffsetType.NEWEST)) {
        return getNewestOffset();
      } else if (offsetType.equals(OffsetType.UPCOMING)) {
        return getUpcomingOffset();
      } else {
        throw new SamzaException("Invalid offset type defined " + offsetType + ".");
      }
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((newestOffset == null) ? 0 : newestOffset.hashCode());
      result = prime * result + ((oldestOffset == null) ? 0 : oldestOffset.hashCode());
      result = prime * result + ((upcomingOffset == null) ? 0 : upcomingOffset.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      SystemStreamPartitionMetadata other = (SystemStreamPartitionMetadata) obj;
      if (newestOffset == null) {
        if (other.newestOffset != null)
          return false;
      } else if (!newestOffset.equals(other.newestOffset))
        return false;
      if (oldestOffset == null) {
        if (other.oldestOffset != null)
          return false;
      } else if (!oldestOffset.equals(other.oldestOffset))
        return false;
      if (upcomingOffset == null) {
        if (other.upcomingOffset != null)
          return false;
      } else if (!upcomingOffset.equals(other.upcomingOffset))
        return false;
      return true;
    }

    @Override
    public String toString() {
      return "SystemStreamPartitionMetadata [oldestOffset=" + oldestOffset + ", newestOffset=" + newestOffset + ", upcomingOffset=" + upcomingOffset + "]";
    }
  }

  /**
   * OffsetType is an enum used to define which offset should be used when
   * reading from a SystemStreamPartition for the first time.
   */
  public enum OffsetType {

    /**
     * Signals the offset of the oldest message in a SystemStreamPartition.
     */
    OLDEST("oldest"),

    /**
     * Signals the offset of the newest message in a SystemStreamPartition.
     */
    NEWEST("newest"),

    /**
     * Signals the offset of the next message to be written into a
     * SystemStreamPartition. If the offset of the most recent message written
     * to a SystemStreamPartition is 7, then upcoming would signal offset 8
     * (assuming the offsets were incremental).
     */
    UPCOMING("upcoming");

    private final String offsetType;

    private OffsetType(String offsetType) {
      this.offsetType = offsetType;
    }
  }
}