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

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.zip.CRC32

import org.apache.samza.util.Util.info

object FileUtil {
  /**
    * Writes checksum & data to a file
    * Checksum is pre-fixed to the data and is a 32-bit long type data.
    * @param file The file handle to write to
    * @param data The data to be written to the file
    * */
  def writeWithChecksum(file: File, data: String) = {
    val checksum = getChecksum(data)
    var oos: ObjectOutputStream = null
    var fos: FileOutputStream = null
    try {
      fos = new FileOutputStream(file)
      oos = new ObjectOutputStream(fos)
      oos.writeLong(checksum)
      oos.writeUTF(data)
    } finally {
      oos.close()
      fos.close()
    }
  }

  /**
    * Reads from a file that has a checksum prepended to the data
    * @param file The file handle to read from
    * */
  def readWithChecksum(file: File): String = {
    var fis: FileInputStream = null
    var ois: ObjectInputStream = null
    try {
      fis = new FileInputStream(file)
      ois = new ObjectInputStream(fis)
      val checksumFromFile = ois.readLong()
      val data = ois.readUTF()
      if(checksumFromFile == getChecksum(data)) {
        data
      } else {
        info("Checksum match failed. Data in file is corrupted. Skipping content.")
        null
      }
    } finally {
      ois.close()
      fis.close()
    }
  }

  /**
    * Recursively remove a directory (or file), and all sub-directories. Equivalent
    * to rm -rf.
    */
  def rm(file: File) {
    if (file == null) {
      return
    } else if (file.isDirectory) {
      val files = file.listFiles()
      if (files != null) {
        for (f <- files)
          rm(f)
      }
      file.delete()
    } else {
      file.delete()
    }
  }

  /**
    * Generates the CRC32 checksum code for any given data
    * @param data The string for which checksum has to be generated
    * @return long type value representing the checksum
    * */
  def getChecksum(data: String) = {
    val crc = new CRC32
    crc.update(data.getBytes)
    crc.getValue
  }
}
