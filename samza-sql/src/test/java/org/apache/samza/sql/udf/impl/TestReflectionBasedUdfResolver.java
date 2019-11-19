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

package org.apache.samza.sql.udf.impl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.udf.ReflectionBasedUdfResolver;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Collection;

public class TestReflectionBasedUdfResolver {

  @Test
  public void testShouldReturnNothingWhenNoUDFIsInPackagePrefix() {
    Config config = new MapConfig(ImmutableMap.of("samza.sql.udf.resolver.package.prefix", "org.apache.samza.udf.blah.blah"));
    ReflectionBasedUdfResolver reflectionBasedUdfResolver = new ReflectionBasedUdfResolver(config);
    Collection<UdfMetadata> udfMetadataList = reflectionBasedUdfResolver.getUdfs();

    Assert.assertEquals(0, udfMetadataList.size());
  }

  @Test
  public void testUDfResolverShouldReturnAllUDFInClassPath() throws NoSuchMethodException {
    Config config = new MapConfig(ImmutableMap.of("samza.sql.udf.resolver.package.prefix", "org.apache.samza.sql.udf.impl"));
    ReflectionBasedUdfResolver reflectionBasedUdfResolver = new ReflectionBasedUdfResolver(config);
    Collection<UdfMetadata> udfMetadataList = reflectionBasedUdfResolver.getUdfs();

    Method method = TestSamzaSqlUdf.class.getMethod("execute", String.class);
    UdfMetadata udfMetadata = new UdfMetadata("TESTSAMZASQLUDF",
            "Test samza sql udf implementation", method, new MapConfig(), ImmutableList.of(SamzaSqlFieldType.STRING),
               SamzaSqlFieldType.STRING, true);

    Assert.assertArrayEquals(new UdfMetadata[]{udfMetadata}, udfMetadataList.toArray(new UdfMetadata[0]));
  }
}
