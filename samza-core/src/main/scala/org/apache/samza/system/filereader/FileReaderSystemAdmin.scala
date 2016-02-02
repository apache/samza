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

import org.apache.samza.system.SystemAdmin
import org.apache.samza.system.SystemStreamMetadata
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata
import scala.collection.JavaConversions._
import java.io.RandomAccessFile
import scala.util.control.Breaks
import org.apache.samza.Partition
import org.apache.samza.util.Logging
import org.apache.samza.SamzaException

class FileReaderSystemAdmin extends SystemAdmin with Logging {
  /**
   * Given a list of streams, get their metadata. This method gets newest offset and upcoming
   * offset by reading the file and then put them into SystemStreamPartitionMetadata. If the
   * file is empty, it will use null for oldest and newest offset and "0" for upcoming offset.
   * The metadata is a map (Partition, SystemStreamPartitionMetadata). Here, we only use one partition
   * for each file. This method returns a map, whose key is stream name and whose value is the metadata.
   *
   * @see getNewestOffsetAndUpcomingOffset(RandomAccessFile)
   */
  def getSystemStreamMetadata(streams: java.util.Set[String]) = {
    val allMetadata = streams.map(stream => {
      val file = new RandomAccessFile(stream, "r")
      val systemStreamPartitionMetadata = file.length match {
        case 0 => new SystemStreamPartitionMetadata(null, null, "0")
        case _ => {
          val (newestOffset, upcomingOffset) = getNewestOffsetAndUpcomingOffset(file)
          new SystemStreamPartitionMetadata("0", newestOffset, upcomingOffset)
        }
      }
      file.close
      val streamPartitionMetadata = Map(new Partition(0) -> systemStreamPartitionMetadata)
      val systemStreamMetadata = new SystemStreamMetadata(stream, streamPartitionMetadata)
      (stream, systemStreamMetadata)
    }).toMap

    info("Got metadata: %s" format allMetadata)

    allMetadata
  }

  /**
   * This method looks for the location of next newline in the file based on supplied offset.
   * It first finds out the \n position of the line which starts with the supplied offset. The
   * next newline's offset will be the \n position + 1.
   *
   * If we can not find the \n position in the supplied offset's line, throw a SamzaException. Because
   * we are supposed to only call this method in fully consumed messages.
   */
  def getOffsetsAfter(offsets: java.util.Map[SystemStreamPartition, String]) = {
    val offsetAfter = offsets.map {
      case (systemStreamPartition, offset) => {
        val file = new RandomAccessFile(systemStreamPartition.getStream, "r")
        val newOffset = findNextEnter(file, offset.toLong, 1) match {
          case Some(x) => x + 1
          case None => throw new SamzaException("the line beginning with " + offset + " in " + systemStreamPartition.getStream + " has not been completed!")
        }
        (systemStreamPartition, newOffset.toString)
      }
    }
    mapAsJavaMap(offsetAfter)
  }

  /**
   * Get the newest offset and upcoming offset from a file. The newest offset is the offset of
   * second-to-last \n in the file + 1. The upcoming offset is the offset of last \n + 1. If
   * there are not enough \n in the file, default value is 0.
   *
   * This method reads file backwards until reach the second-to-last \n. The assumption is, in most cases,
   * there are more bytes to second-to-last \n from beginning than from ending.
   */
  private def getNewestOffsetAndUpcomingOffset(file: RandomAccessFile): (String, String) = {
    var newestOffset = 0
    val upcomingOffset = findNextEnter(file, file.length - 1, -1) match {
      case Some(x) => x + 1
      case None => 0
    }
    // if we can not find upcomingOffset, we can not find newest offset either.
    if (upcomingOffset != 0) {
      // upcomingOffset - 2 is the offset of the byte before the last \n
      newestOffset = findNextEnter(file, upcomingOffset - 2, -1) match {
        case Some(x) => x + 1
        case None => 0
      }
    }
    (newestOffset.toString, upcomingOffset.toString)
  }

  /**
   * This method is to find the next \n in the file according to the starting position provided.
   * If the step is +1, it will look for the \n after this position; if the step is -1, it will
   * look for the \n before this starting position. If it finds the \n, return the position of
   * this \n, otherwise, it returns -1.
   */
  private def findNextEnter(file: RandomAccessFile, startingPosition: Long, step: Int): Option[Int] = {
    var enterPosition: Option[Int] = None
    var i = startingPosition
    val loop = new Breaks
    loop.breakable(
      while (i < file.length && i > -1) {
        file.seek(i)
        val cha = file.read.toChar
        if (cha == '\n') {
          enterPosition = Some(i.toInt)
          loop.break
        }
        i = i + step
      })
    enterPosition
  }

  def createChangelogStream(topicName: String, numOfChangeLogPartitions: Int) = {
    throw new UnsupportedOperationException("Method not implemented.")
  }

  def validateChangelogStream(topicName: String, numOfChangeLogPartitions: Int) = {
    throw new UnsupportedOperationException("Method not implemented.")
  }

  def createCoordinatorStream(streamName: String) {
    throw new UnsupportedOperationException("Method not implemented.")
  }

  override def offsetComparator(offset1: String , offset2: String) = {
    null
  }
}