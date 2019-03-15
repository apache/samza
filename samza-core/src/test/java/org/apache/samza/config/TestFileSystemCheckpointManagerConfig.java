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
