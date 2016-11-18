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

package org.apache.samza.system.hdfs.reader;

import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of the HdfsReader that reads and processes avro format
 * files.
 */
public class AvroFileHdfsReader implements SingleFileHdfsReader {

  private static final Logger LOG = LoggerFactory.getLogger(AvroFileHdfsReader.class);

  private final SystemStreamPartition systemStreamPartition;
  private DataFileReader<GenericRecord> fileReader;
  private long curBlockStart;
  private long curRecordOffset;

  public AvroFileHdfsReader(SystemStreamPartition systemStreamPartition) {
    this.systemStreamPartition = systemStreamPartition;
    this.fileReader = null;
  }

  @Override
  public void open(String pathStr, String singleFileOffset) {
    LOG.info(String.format("%s: Open file [%s] with file offset [%s] for read", systemStreamPartition, pathStr, singleFileOffset));
    Path path = new Path(pathStr);
    try {
      AvroFSInput input = new AvroFSInput(FileContext.getFileContext(path.toUri()), path);
      fileReader = new DataFileReader<>(input, new GenericDatumReader<>());
      seek(singleFileOffset);
    } catch (IOException e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public void seek(String singleFileOffset) {
    try {
      // See comments for AvroFileCheckpoint to understand the behavior below
      AvroFileCheckpoint checkpoint = new AvroFileCheckpoint(singleFileOffset);
      if (checkpoint.isStartingOffset()) {
        // seek to the beginning of the first block
        fileReader.sync(0);
        curBlockStart = fileReader.previousSync();
        curRecordOffset = 0;
        return;
      }
      fileReader.seek(checkpoint.getBlockStart());
      for (int i = 0; i < checkpoint.getRecordOffset(); i++) {
        if (fileReader.hasNext()) {
          fileReader.next();
        }
      }
      curBlockStart = checkpoint.getBlockStart();
      curRecordOffset = checkpoint.getRecordOffset();
    } catch (IOException e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public IncomingMessageEnvelope readNext() {
    // get checkpoint for THIS record
    String checkpoint = nextOffset();
    GenericRecord record = fileReader.next();
    if (fileReader.previousSync() != curBlockStart) {
      curBlockStart = fileReader.previousSync();
      curRecordOffset = 0;
    } else {
      curRecordOffset++;
    }
    // avro schema doesn't necessarily have key field
    return new IncomingMessageEnvelope(systemStreamPartition, checkpoint, null, record);
  }

  @Override
  public boolean hasNext() {
    return fileReader.hasNext();
  }

  @Override
  public void close() {
    LOG.info("About to close file reader for " + systemStreamPartition);
    try {
      fileReader.close();
    } catch (IOException e) {
      throw new SamzaException(e);
    }
    LOG.info("File reader closed for " + systemStreamPartition);
  }

  @Override
  public String nextOffset() {
    return AvroFileCheckpoint.generateCheckpointStr(curBlockStart, curRecordOffset);
  }

  public static int offsetComparator(String offset1, String offset2) {
    AvroFileCheckpoint cp1 = new AvroFileCheckpoint(offset1);
    AvroFileCheckpoint cp2 = new AvroFileCheckpoint(offset2);
    return cp1.compareTo(cp2);
  }

  /**
   * An avro file looks something like this:
   *
   * Byte offset: 0       103            271         391
   *              ┌────────┬──────────────┬───────────┬───────────┐
   * Avro file:   │ Header │    Block 1   │  Block 2  │  Block 3  │ ...
   *              └────────┴──────────────┴───────────┴───────────┘
   *
   * Each block contains multiple records. The start of a block is defined as a valid
   * synchronization point. A file reader can only seek to a synchronization point, i.e.
   * the start of blocks. Thus, to precisely describe the location of a record, we need
   * to use the pair (blockStart, recordOffset). Here "blockStart" means the start of the
   * block and "recordOffset" means the index of the record within the block.
   * Take the example above, and suppose block 1 has 4 records, we have record sequences as:
   * (103, 0), (103, 1), (103, 2), (103, 3), (271, 0), ...
   * where (271, 0) represents the first event in block 2
   *
   * With the CP_DELIM being '@', the actual checkpoint string would look like "103@1",
   * "271@0" or "271", etc. For convenience, a checkpoint with only the blockStart but no
   * recordOffset within the block simply means the first record in that block. Thus,
   * "271@0" is equal to "271".
   */
  public static class AvroFileCheckpoint {
    private static final String CP_DELIM = "@";
    private long blockStart; // start position of the block
    private long recordOffset; // record offset within the block
    String checkpointStr;

    public static String generateCheckpointStr(long blockStart, long recordOffset) {
      return blockStart + CP_DELIM + recordOffset;
    }

    public AvroFileCheckpoint(String checkpointStr) {
      String[] elements = checkpointStr.replaceAll("\\s", "").split(CP_DELIM);
      if (elements.length > 2 || elements.length < 1) {
        throw new SamzaException("Invalid checkpoint for AvroFileHdfsReader: " + checkpointStr);
      }
      try {
        blockStart = Long.parseLong(elements[0]);
        recordOffset = elements.length == 2 ? Long.parseLong(elements[1]) : 0;
      } catch (NumberFormatException e) {
        throw new SamzaException("Invalid checkpoint for AvroFileHdfsReader: " + checkpointStr, e);
      }
      this.checkpointStr = checkpointStr;
    }

    public AvroFileCheckpoint(long blockStart, long recordOffset) {
      this.blockStart = blockStart;
      this.recordOffset = recordOffset;
      this.checkpointStr = generateCheckpointStr(blockStart, recordOffset);
    }

    public long getBlockStart() {
      return blockStart;
    }

    public long getRecordOffset() {
      return recordOffset;
    }

    public String getCheckpointStr() {
      return checkpointStr;
    }

    public boolean isStartingOffset() {
      return blockStart == 0;
    }

    public int compareTo(AvroFileCheckpoint other) {
      if (this.blockStart < other.blockStart) {
        return -1;
      } else if (this.blockStart > other.blockStart) {
        return 1;
      } else return Long.compare(this.recordOffset, other.recordOffset);
    }

    @Override
    public String toString() {
      return getCheckpointStr();
    }
  }
}
