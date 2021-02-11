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

package org.apache.samza.system.filereader

import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.Partition
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.AssertionsForJUnit

import scala.collection.JavaConverters._
import scala.collection.mutable

class TestFileReaderSystemAdmin extends AssertionsForJUnit {

  val files = List(
    getClass.getResource("/empty.txt").getPath,
    getClass.getResource("/noEnter.txt").getPath,
    getClass.getResource("/oneEnter.txt").getPath,
    getClass.getResource("/twoEnter.txt").getPath,
    getClass.getResource("/moreEnter.txt").getPath)

  @Test
  def testGetOffsetsAfter {
    val fileReaderSystemAdmin = new FileReaderSystemAdmin
    val ssp3 = new SystemStreamPartition("file-reader", files(2), new Partition(0))
    val ssp4 = new SystemStreamPartition("file-reader", files(3), new Partition(0))
    val ssp5 = new SystemStreamPartition("file-reader", files(4), new Partition(0))

    val offsets: java.util.Map[SystemStreamPartition, String] =
      mutable.HashMap(ssp3 -> "0", ssp4 -> "12", ssp5 -> "25").asJava
    val afterOffsets = fileReaderSystemAdmin.getOffsetsAfter(offsets)
    assertEquals("11", afterOffsets.get(ssp3))
    assertEquals("23", afterOffsets.get(ssp4))
    assertEquals("34", afterOffsets.get(ssp5))
  }

  @Test
  def testGetSystemStreamMetadata {
    val fileReaderSystemAdmin = new FileReaderSystemAdmin
    val allMetadata = fileReaderSystemAdmin.getSystemStreamMetadata(setAsJavaSetConverter(files.toSet).asJava)
    val expectedEmpty = new SystemStreamPartitionMetadata(null, null, "0")
    val expectedNoEntry = new SystemStreamPartitionMetadata("0", "0", "0")
    val expectedOneEntry = new SystemStreamPartitionMetadata("0", "0", "11")
    val expectedTwoEntry = new SystemStreamPartitionMetadata("0", "11", "23")
    val expectedMoreEntry = new SystemStreamPartitionMetadata("0", "34", "46")

    allMetadata.asScala.foreach { entry =>
      {
        val result = entry._2.getSystemStreamPartitionMetadata.get(new Partition(0))
        if (entry._1.endsWith("empty.txt")) {
          assertEquals(expectedEmpty, result)
        } else if (entry._1.endsWith("noEnter.txt")) {
          assertEquals(expectedNoEntry, result)
        } else if (entry._1.endsWith("oneEnter.txt")) {
          assertEquals(expectedOneEntry, result)
        } else if (entry._1.endsWith("twoEnter.txt")) {
          assertEquals(expectedTwoEntry, result)
        } else if (entry._1.endsWith("moreEnter.txt")) {
          assertEquals(expectedMoreEntry, result)
        } else
          fail()
      }
    }
  }
}
