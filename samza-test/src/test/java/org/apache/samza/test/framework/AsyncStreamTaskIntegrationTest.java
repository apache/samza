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

package org.apache.samza.test.framework;

import java.util.Arrays;
import java.util.List;
import org.apache.samza.test.framework.stream.CollectionStream;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Test;


public class AsyncStreamTaskIntegrationTest {

  @Test
  public void testAsyncTaskWithSinglePartition() throws Exception {
    List<Integer> inputList = Arrays.asList(1, 2, 3, 4, 5);
    List<Integer> outputList = Arrays.asList(10, 20, 30, 40, 50);

    CollectionStream<Integer> input = CollectionStream.of("async-test", "ints", inputList);
    CollectionStream output = CollectionStream.empty("async-test", "ints-out");

    TestRunner
        .of(MyAsyncStreamTask.class)
        .addInputStream(input)
        .addOutputStream(output)
        .run(1500);

    Assert.assertThat(TestRunner.consumeStream(output, 1000).get(0),
        IsIterableContainingInOrder.contains(outputList.toArray()));
  }
}
