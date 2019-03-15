package org.apache.samza.config;

import java.util.Optional;


public class FileSystemCheckpointManagerConfig extends MapConfig {
  /**
   * Path on local file system where checkpoints should be stored.
   */
  private static final String CHECKPOINT_MANAGER_ROOT = "task.checkpoint.path";

  public FileSystemCheckpointManagerConfig(Config config) {
    super(config);
  }

  public Optional<String> getFileSystemCheckpointRoot() {
    return Optional.ofNullable(get(CHECKPOINT_MANAGER_ROOT));
  }
}
