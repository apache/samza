/*
 *
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
 *
 */

package org.apache.samza.util

import org.apache.samza.testUtils.FileUtil

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import org.junit.Assert.{assertEquals, assertNull, assertTrue, fail}
import org.junit.Test

import java.nio.file.{FileAlreadyExistsException, Files, Paths}
import scala.util.Random

class TestFileUtil {
  val data = "100"
  val fileUtil = new FileUtil()
  val checksum = fileUtil.getChecksum(data)
  val file = new File(System.getProperty("java.io.tmpdir"), "test")

  @Test
  def testWriteDataToFile() {
    // Invoke test
    fileUtil.writeWithChecksum(file, data)

    // Check that file exists
    assertTrue("File was not created!", file.exists())
    val fis = new FileInputStream(file)
    val ois = new ObjectInputStream(fis)

    // Check content of the file is as expected
    assertEquals(checksum, ois.readLong())
    assertEquals(data, ois.readUTF())
    ois.close()
    fis.close()
  }

  @Test
  def testWriteDataToFileWithExistingOffsetFile() {
    // Invoke test
    val file = new File(System.getProperty("java.io.tmpdir"), "test2")
    // write the same file three times
    fileUtil.writeWithChecksum(file, data)
    fileUtil.writeWithChecksum(file, data)
    fileUtil.writeWithChecksum(file, data)

    // Check that file exists
    assertTrue("File was not created!", file.exists())
    val fis = new FileInputStream(file)
    val ois = new ObjectInputStream(fis)

    // Check content of the file is as expected
    assertEquals(checksum, ois.readLong())
    assertEquals(data, ois.readUTF())
    ois.close()
    fis.close()
  }


  @Test
  def testReadDataFromFile() {
    // Setup
    val fos = new FileOutputStream(file)
    val oos = new ObjectOutputStream(fos)
    oos.writeLong(checksum)
    oos.writeUTF(data)
    oos.close()
    fos.close()

    // Invoke test
    val result = fileUtil.readWithChecksum(file)

    // Check data returned
    assertEquals(data, result)
  }

  @Test
  def testReadInvalidDataFromFile() {
    // Write garbage to produce a null result when it's read
    val fos = new FileOutputStream(file)
    val oos = new ObjectOutputStream(fos)
    oos.writeLong(1)
    oos.writeUTF("Junk Data")
    oos.close()
    fos.close()

    // Invoke test
    val result = fileUtil.readWithChecksum(file)

    // Check data returned
    assertNull(result)
  }

  /**
   * Files.createDirectories fails with a FileAlreadyExistsException if the last directory
   * in the path already exists but is a symlink to another directory. It works correctly
   * if one of the intermediate directory is a symlink. Verify this behavior and
   * test that the util method handles this correctly.
   */
  @Test
  def testCreateDirectoriesWithSymlinks(): Unit = {
    /**
     * Directory structure:
     * /tmp/samza-file-util-RANDOM
     * /tmp/samza-file-util-RANDOM-symlink (symlink to dir above)
     * /tmp/samza-file-util-RANDOM/subdir (created via the symlink above)
     */
    val tmpDirPath = Paths.get(FileUtil.TMP_DIR)
    val tmpSubDirName = "samza-file-util-" + Random.nextInt()
    val tmpSubDirSymlinkName = tmpSubDirName + "-symlink"

    val tmpSubDirPath = Paths.get(FileUtil.TMP_DIR, tmpSubDirName);
    fileUtil.createDirectories(tmpSubDirPath)

    val tmpSymlinkPath = Paths.get(FileUtil.TMP_DIR, tmpSubDirSymlinkName)
    Files.createSymbolicLink(tmpSymlinkPath, tmpDirPath);

    try {
      Files.createDirectories(tmpSymlinkPath)
      fail("Should have thrown a FileAlreadyExistsException since last dir in path already " +
        "exists and is a symlink")
    } catch {
      case e: FileAlreadyExistsException =>
        // ignore and continue
    }

    // test that the util method handles this correctly and does not throw an exception
    fileUtil.createDirectories(tmpSymlinkPath)

    // verify that subdirs can be created via symlinks correctly.
    val tmpSubSubDirPath = Paths.get(FileUtil.TMP_DIR, tmpSubDirName + "-symlink", "subdir")
    fileUtil.createDirectories(tmpSubSubDirPath)
  }
}
