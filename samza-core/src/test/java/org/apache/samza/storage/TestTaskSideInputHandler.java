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
package org.apache.samza.storage;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.Clock;
import org.apache.samza.util.ScalaJavaUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;


public class TestTaskSideInputHandler {
  private static final String TEST_SYSTEM = "test-system";
  private static final String TEST_STORE = "test-store";
  private static final String TEST_STREAM = "test-stream";

    /**
   * This test is for cases, when calls to systemAdmin (e.g., KafkaSystemAdmin's) get-stream-metadata method return null.
   */
  @Test
  public void testGetStartingOffsetsWhenStreamMetadataIsNull() {
    final String taskName = "test-get-starting-offset-task";

    Set<SystemStreamPartition> ssps = IntStream.range(1, 2)
        .mapToObj(idx -> new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(idx)))
        .collect(Collectors.toSet());
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetadata = ssps.stream()
        .collect(Collectors.toMap(SystemStreamPartition::getPartition,
          x -> new SystemStreamMetadata.SystemStreamPartitionMetadata(null, "1", "2")));


    TaskSideInputHandler handler = new MockTaskSideInputHandlerBuilder(taskName, TaskMode.Active)
        .addStreamMetadata(Collections.singletonMap(new SystemStream(TEST_SYSTEM, TEST_STREAM),
            new SystemStreamMetadata(TEST_STREAM, partitionMetadata)))
        .addStore(TEST_STORE, ssps)
        .build();

    handler.init();

    ssps.forEach(ssp -> {
      String startingOffset = handler.getStartingOffset(
          new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, ssp.getPartition()));
      Assert.assertNull("Starting offset should be null", startingOffset);
    });
  }

  @Test
  public void testGetStartingOffsets() {
    final String storeName = "test-get-starting-offset-store";
    final String taskName = "test-get-starting-offset-task";

    Set<SystemStreamPartition> ssps = IntStream.range(1, 6)
        .mapToObj(idx -> new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(idx)))
        .collect(Collectors.toSet());


    TaskSideInputHandler handler = new MockTaskSideInputHandlerBuilder(taskName, TaskMode.Active)
        .addStore(storeName, ssps)
        .build();

    // set up file and oldest offsets. for even partitions, fileOffsets will be larger; for odd partitions oldestOffsets will be larger
    Map<SystemStreamPartition, String> fileOffsets = ssps.stream()
        .collect(Collectors.toMap(Function.identity(), ssp -> {
          int partitionId = ssp.getPartition().getPartitionId();
          int offset = partitionId % 2 == 0 ? partitionId + 10 : partitionId;
          return String.valueOf(offset);
        }));
    Map<SystemStreamPartition, String> oldestOffsets = ssps.stream()
        .collect(Collectors.toMap(Function.identity(), ssp -> {
          int partitionId = ssp.getPartition().getPartitionId();
          int offset = partitionId % 2 == 0 ? partitionId : partitionId + 10;

          return String.valueOf(offset);
        }));

    doCallRealMethod().when(handler).getStartingOffsets(fileOffsets, oldestOffsets);

    Map<SystemStreamPartition, String> startingOffsets = handler.getStartingOffsets(fileOffsets, oldestOffsets);

    assertTrue("Failed to get starting offsets for all ssps", startingOffsets.size() == 5);
    startingOffsets.forEach((ssp, offset) -> {
      int partitionId = ssp.getPartition().getPartitionId();
      String expectedOffset = partitionId % 2 == 0
          // 1 + fileOffset
          ? getOffsetAfter(String.valueOf(ssp.getPartition().getPartitionId() + 10))
          // oldestOffset
          : String.valueOf(ssp.getPartition().getPartitionId() + 10);
      assertEquals("Larger of fileOffsets and oldestOffsets should always be chosen", expectedOffset, offset);
    });
  }

  private static final class MockTaskSideInputHandlerBuilder {
    final TaskName taskName;
    final TaskMode taskMode;
    final File storeBaseDir;

    final Map<String, StorageEngine> stores = new HashMap<>();
    final Map<String, Set<SystemStreamPartition>> storeToSSPs = new HashMap<>();
    final Clock clock = mock(Clock.class);
    final Map<String, SideInputsProcessor> storeToProcessor = new HashMap<>();
    final StreamMetadataCache streamMetadataCache = mock(StreamMetadataCache.class);
    final SystemAdmins systemAdmins = mock(SystemAdmins.class);

    public MockTaskSideInputHandlerBuilder(String taskName, TaskMode taskMode) {
      this.taskName = new TaskName(taskName);
      this.taskMode = taskMode;
      this.storeBaseDir = mock(File.class);

      initializeMocks();
    }

    private void initializeMocks() {
      SystemAdmin admin = mock(SystemAdmin.class);
      doAnswer(invocation -> {
        String offset1 = invocation.getArgumentAt(0, String.class);
        String offset2 = invocation.getArgumentAt(1, String.class);

        return Long.compare(Long.parseLong(offset1), Long.parseLong(offset2));
      }).when(admin).offsetComparator(any(), any());
      doAnswer(invocation -> {
        Map<SystemStreamPartition, String> sspToOffsets = invocation.getArgumentAt(0, Map.class);

        return sspToOffsets.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> getOffsetAfter(entry.getValue())));
      }).when(admin).getOffsetsAfter(any());
      doReturn(admin).when(systemAdmins).getSystemAdmin(TEST_SYSTEM);
      doReturn(ScalaJavaUtil.toScalaMap(new HashMap<>())).when(streamMetadataCache).getStreamMetadata(any(), anyBoolean());
    }


    MockTaskSideInputHandlerBuilder addStreamMetadata(Map<SystemStream, SystemStreamMetadata> streamMetadata) {
      doReturn(ScalaJavaUtil.toScalaMap(streamMetadata)).when(streamMetadataCache).getStreamMetadata(any(), anyBoolean());
      return this;
    }

    MockTaskSideInputHandlerBuilder addStore(String storeName, Set<SystemStreamPartition> storeSSPs) {
      storeToSSPs.put(storeName, storeSSPs);
      storeToProcessor.put(storeName, mock(SideInputsProcessor.class));
      return this;
    }

    TaskSideInputHandler build() {
      return spy(new TaskSideInputHandler(taskName,
          taskMode,
          storeBaseDir,
          stores,
          storeToSSPs,
          storeToProcessor,
          systemAdmins,
          streamMetadataCache,
          new CountDownLatch(1),
          clock, ImmutableMap.of(), ImmutableMap.of()));
    }
  }

  private static String getOffsetAfter(String offset) {
    return String.valueOf(Long.parseLong(offset) + 1);
  }
}
