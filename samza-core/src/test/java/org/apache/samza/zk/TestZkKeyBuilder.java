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
package org.apache.samza.zk;

import org.junit.Assert;
import org.junit.Test;

public class TestZkKeyBuilder {

  @Test(expected = IllegalArgumentException.class)
  public void pathPrefixCannotBeNull() {
    new ZkKeyBuilder(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void pathPrefixCannotBeEmpty() {
    new ZkKeyBuilder("    ");
  }

  @Test
  public void testProcessorsPath() {
    ZkKeyBuilder builder = new ZkKeyBuilder("test");
    Assert.assertEquals("/test/" + ZkKeyBuilder.PROCESSORS_PATH, builder.getProcessorsPath());
  }

  @Test
  public void testParseIdFromPath() {
    Assert.assertEquals(
        "1",
        ZkKeyBuilder.parseIdFromPath("/test/processors/" + "1"));
    Assert.assertNull(ZkKeyBuilder.parseIdFromPath(null));
    Assert.assertNull(ZkKeyBuilder.parseIdFromPath(""));
  }

  @Test
  public void testJobModelPath() {
    ZkKeyBuilder builder = new ZkKeyBuilder("test");

    Assert.assertEquals("/test/" + ZkKeyBuilder.JOBMODEL_GENERATION_PATH + "/jobModelVersion", builder.getJobModelVersionPath());
    Assert.assertEquals("/test/" + ZkKeyBuilder.JOBMODEL_GENERATION_PATH + "/jobModels", builder.getJobModelPathPrefix());
    String version = "2";
    Assert.assertEquals("/test/" + ZkKeyBuilder.JOBMODEL_GENERATION_PATH + "/jobModels/" + version, builder.getJobModelPath(version));
    Assert.assertEquals("/test/" + ZkKeyBuilder.JOBMODEL_GENERATION_PATH + "/testBarrier/versionBarriers", builder.getJobModelVersionBarrierPrefix(
        "testBarrier"));
  }
}
