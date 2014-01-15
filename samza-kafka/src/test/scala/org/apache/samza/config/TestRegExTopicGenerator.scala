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

import org.junit.Test
import collection.JavaConversions._
import org.junit.Assert._
import KafkaConfig._
import org.apache.samza.SamzaException

class TestRegExTopicGenerator {

  private def REWRITER_NAME = "test"
  private def getRegexConfigKey = REGEX_RESOLVED_STREAMS format REWRITER_NAME
  private def getRegexConfigSystem = REGEX_RESOLVED_SYSTEM format REWRITER_NAME
  private def getRegexConfigInherited = REGEX_INHERITED_CONFIG format REWRITER_NAME

  @Test
  def configsAreRewrittenCorrectly = {
    val unrelated = "unrelated.key.howdy" -> "should.survive"
    val map = Map(
      getRegexConfigKey -> ".*cat",
      getRegexConfigSystem -> "test",
      getRegexConfigInherited + ".ford" -> "mustang",
      getRegexConfigInherited + ".alfa.romeo" -> "spider",
      getRegexConfigInherited + ".b.triumph" -> "spitfire",
      unrelated)

    val config = new MapConfig(map)

    // Don't actually talk to ZooKeeper
    val rewriter = new RegExTopicGenerator() {
      override def getTopicsFromZK(rewriterName: String, config: Config): Seq[String] = List("catdog", "dogtired", "cow", "scaredycat", "Homer", "crazycat")
    }

    val rewritten = rewriter.rewrite(REWRITER_NAME, config)

    val expected = Map(
      "task.inputs" -> "test.crazycat,test.scaredycat",
      "systems.test.streams.scaredycat.ford" -> "mustang",
      "systems.test.streams.scaredycat.alfa.romeo" -> "spider",
      "systems.test.streams.scaredycat.b.triumph" -> "spitfire",
      "systems.test.streams.crazycat.ford" -> "mustang",
      "systems.test.streams.crazycat.alfa.romeo" -> "spider",
      "systems.test.streams.crazycat.b.triumph" -> "spitfire",
      unrelated)

    expected.foreach(e => assertEquals(e._2, rewritten.get(e._1))) // Compiler bug in 2.8 requires this dumb syntax
    val inputStreams = rewritten.get(TaskConfig.INPUT_STREAMS).split(",").toSet
    assertEquals(2, inputStreams.size)
    assertEquals(Set("test.crazycat", "test.scaredycat"), inputStreams)
  }

  @Test
  def emptyInputStreamsWorks = {
    // input.streams is required but appears as the empty string, which has been problematic.
    val map = Map(
      TaskConfig.INPUT_STREAMS -> "",
      getRegexConfigKey -> "yo.*",
      getRegexConfigSystem -> "test",
      getRegexConfigInherited + ".config.zorp" -> "morp")

    val rewriter = new RegExTopicGenerator() {
      override def getTopicsFromZK(rewriterName: String, config: Config): Seq[String] = List("yoyoyo")
    }

    val config = rewriter.rewrite(REWRITER_NAME, new MapConfig(map))
    assertEquals("test.yoyoyo", config.get(TaskConfig.INPUT_STREAMS))
  }
}
