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
package org.apache.samza.container.disk;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class TestPollingScanDiskSpaceMonitor {
  private Path testDir;
  private ArrayDeque<Path> filesToDelete;

  @Before
  public void setUp() throws IOException {
    filesToDelete = new ArrayDeque<>();
    testDir = Files.createTempDirectory("samza-polling-scan-disk-monitor-test");
    filesToDelete.push(testDir);
  }

  @After
  public void tearDown() throws IOException {
    while (!filesToDelete.isEmpty()) {
      Path path = filesToDelete.pop();

      try {
        Files.delete(path);
      } catch (IOException e) {
        // Continue with best effort, this is just test code.
      }
    }
  }

  @Test
  public void testSizeOfSingleFile() throws IOException {
    writeFile(testDir, "single-file", new byte[1024]);
    assertEquals(1024, PollingScanDiskSpaceMonitor.getSpaceUsed(Collections.singleton(testDir)));
  }

  @Test
  public void testSizeOfDisjointDirectoriesFromRoot() throws IOException {
    Path child1Dir = createDirectory(testDir, "child1");
    writeFile(child1Dir, "foo", new byte[1024]);

    Path child2Dir = createDirectory(testDir, "child2");
    writeFile(child2Dir, "bar", new byte[4096]);

    assertEquals(1024 + 4096, PollingScanDiskSpaceMonitor.getSpaceUsed(Collections.singleton(testDir)));
  }

  @Test
  public void testSizeOfDisjointDirectoriesFromChildDirs() throws IOException {
    Path child1Dir = createDirectory(testDir, "child1");
    writeFile(child1Dir, "foo", new byte[1024]);

    Path child2Dir = createDirectory(testDir, "child2");
    writeFile(child2Dir, "bar", new byte[4096]);

    Set<Path> pathSet = new HashSet<>(Arrays.asList(child1Dir, child2Dir));
    assertEquals(1024 + 4096, PollingScanDiskSpaceMonitor.getSpaceUsed(pathSet));
  }

  @Test
  public void testSizeOfOverlappedDirectories() throws IOException {
    Path childDir = createDirectory(testDir, "child");
    writeFile(childDir, "foo", new byte[1024]);

    Path grandchildDir = createDirectory(childDir, "grandchild");
    writeFile(grandchildDir, "bar", new byte[4096]);

    // If getSpaceUsed were not handling overlapping directories we would expect to count
    // grandchild twice, which would give us the erroneous total `1024 + 4096 * 2`.
    Set<Path> pathSet = new HashSet<>(Arrays.asList(childDir, grandchildDir));
    assertEquals(1024 + 4096, PollingScanDiskSpaceMonitor.getSpaceUsed(pathSet));
  }

  @Test
  public void testSizeOfDirectoryAccessedWithDifferentPaths() throws IOException {
    Path childDir = createDirectory(testDir, "child1");
    writeFile(childDir, "foo", new byte[1024]);

    Path otherPath = childDir.resolve("..").resolve(childDir.getFileName());
    Set<Path> pathSet = new HashSet<>(Arrays.asList(childDir, otherPath));

    // This test actually verifies that !childDir.equals(otherPath) and ensures that we properly
    // handle duplicate paths to the same directory.
    assertEquals(1024, PollingScanDiskSpaceMonitor.getSpaceUsed(pathSet));
  }

  @Test
  public void testSizeOfAlreadyCountedSymlinkedFile() throws IOException {
    writeFile(testDir, "regular-file", new byte[1024]);
    Files.createSymbolicLink(testDir.resolve("symlink"), testDir.resolve("regular-file"));

    // We should not double count a symlinked file
    assertEquals(1024, PollingScanDiskSpaceMonitor.getSpaceUsed(Collections.singleton(testDir)));
  }

  @Test
  public void testSizeOfUncountedSymlinkedFile() throws IOException {
    Path childDir = createDirectory(testDir, "child");
    writeFile(testDir, "regular-file", new byte[1024]);
    Files.createSymbolicLink(childDir.resolve("symlink"), testDir.resolve("regular-file"));

    // We should count the space of the symlinked file even thought it is outside of the root
    // from which we started.
    assertEquals(1024, PollingScanDiskSpaceMonitor.getSpaceUsed(Collections.singleton(testDir)));
  }

  @Test
  public void testFollowSymlinkedDirectory() throws IOException {
    Path childDir = createDirectory(testDir, "child");
    writeFile(childDir, "regular-file", new byte[1024]);

    Path dirSymlink = testDir.resolve("symlink");
    Files.createSymbolicLink(dirSymlink, childDir);

    // We should follow the symlink and read the symlinked file
    assertEquals(1024, PollingScanDiskSpaceMonitor.getSpaceUsed(Collections.singleton(dirSymlink)));
  }

  @Test
  public void testHandleCyclicalSymlink() throws IOException {
    Path childDir = createDirectory(testDir, "child");
    writeFile(childDir, "regular-file", new byte[1024]);
    Files.createSymbolicLink(childDir.resolve("symlink"), testDir);

    // We have testDir/childDir/symlink -> testDir, which effectively creates a cycle.
    assertEquals(1024, PollingScanDiskSpaceMonitor.getSpaceUsed(Collections.singleton(childDir)));
  }

  @Test
  public void testMissingDirectory() throws IOException {
    Set<Path> pathSet = Collections.singleton(testDir.resolve("non-existant-child"));
    assertEquals(0, PollingScanDiskSpaceMonitor.getSpaceUsed(pathSet));
  }

  @Test
  public void testGetSamplesFromListener() throws IOException, InterruptedException {
    writeFile(testDir, "single-file", new byte[1024]);

    final AtomicLong sample = new AtomicLong();
    final CountDownLatch sampleReady = new CountDownLatch(1);
    final PollingScanDiskSpaceMonitor monitor = new PollingScanDiskSpaceMonitor(Collections.singleton(testDir), 50);
    monitor.registerListener(new DiskSpaceMonitor.Listener() {
      @Override
      public void onUpdate(long diskUsageSample) {
        sample.set(diskUsageSample);
        sampleReady.countDown();
      }
    });

    monitor.start();

    try {
      if (!sampleReady.await(5, TimeUnit.SECONDS)) {
        fail("Timed out waiting for listener to be provide disk usage sample");
      }

      assertEquals(1024, sample.get());
    } finally {
      monitor.stop();
    }
  }

  @Test
  public void testStartStop() throws IOException, InterruptedException {
    writeFile(testDir, "single-file", new byte[1024]);

    final int numSamplesToCollect = 5;

    final AtomicInteger numCallbackInvocations = new AtomicInteger();
    final CountDownLatch doneLatch = new CountDownLatch(1);
    final PollingScanDiskSpaceMonitor monitor = new PollingScanDiskSpaceMonitor(Collections.singleton(testDir), 50);
    monitor.registerListener(new DiskSpaceMonitor.Listener() {
      @Override
      public void onUpdate(long diskUsageSample) {
        if (numCallbackInvocations.incrementAndGet() == numSamplesToCollect) {
          monitor.stop();
          doneLatch.countDown();
        }
      }
    });

    monitor.start();

    try {
      if (!doneLatch.await(5, TimeUnit.SECONDS)) {
        fail(String.format("Timed out waiting for listener to be give %d updates", numSamplesToCollect));
      }
      if (!monitor.awaitTermination(5, TimeUnit.SECONDS)) {
        fail("Timed out waiting for monitor to terminate");
      }

      // A number larger than numSamplesToCollect indicates that we got a callback after we stopped
      // the monitor. We should safely be able to assert this will not happen as we stopped the
      // monitor in the the thread on which it is delivering notifications.
      assertEquals(numSamplesToCollect, numCallbackInvocations.get());
    } finally {
      monitor.stop();
    }
  }

  private Path createDirectory(Path parentDir, String name) throws IOException {
    name = name + "-";
    Path path = Files.createTempDirectory(parentDir, name);
    filesToDelete.push(path);
    return path;
  }

  private Path createFile(Path parentDir, String name) throws IOException {
    name = name + "-";
    Path path = Files.createTempFile(parentDir, name, null);
    filesToDelete.push(path);
    return path;
  }

  private void writeFile(Path parentDir, String name, byte[] contents) throws IOException {
    Path path = createFile(parentDir, name);
    Files.write(path, contents);
  }
}
