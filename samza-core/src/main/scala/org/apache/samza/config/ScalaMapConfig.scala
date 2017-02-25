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

package org.apache.samza.config
import org.apache.samza.SamzaException

class ScalaMapConfig(config: Config) extends MapConfig(config) {
  def getOrElse(k: String, els: String) = getOption(k).getOrElse(els)

  def getOption(k: String): Option[String] = if (containsKey(k)) Some(config.get(k)) else None

  def getNonEmptyOption(k: String): Option[String] = {
    getOption(k) match {
      case Some(v: String) if (!v.isEmpty) => Some(v)
      case _ => None
    }
  }


  def getExcept(k: String, msg: String = null): String = 
    getOption(k) match {
      case Some(s) => s
      case _ =>
        val error = 
          if(msg == null) "Missing required configuration '%s'".format(k)
          else "Missing required configuration '%s': %s".format(k, msg)
        throw new SamzaException(error)
    }
}
