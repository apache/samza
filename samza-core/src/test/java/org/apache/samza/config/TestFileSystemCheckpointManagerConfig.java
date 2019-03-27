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
package org.apache.samza.config;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class TestFileSystemCheckpointManagerConfig {
  @Test
  public void testGetFileSystemCheckpointRoot() {
    String checkpointManagerRoot = "checkpointManagerRoot";

    // checkpoint path exists
    Config config = new MapConfig(ImmutableMap.of("task.checkpoint.path", checkpointManagerRoot));
    FileSystemCheckpointManagerConfig fileSystemCheckpointManagerConfig = new FileSystemCheckpointManagerConfig(config);
    assertEquals(checkpointManagerRoot, fileSystemCheckpointManagerConfig.getFileSystemCheckpointRoot().get());

    // checkpoint path does not exist
    config = new MapConfig();
    fileSystemCheckpointManagerConfig = new FileSystemCheckpointManagerConfig(config);
    assertFalse(fileSystemCheckpointManagerConfig.getFileSystemCheckpointRoot().isPresent());
  }
}
