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

package org.apache.samza.test.table;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.ByteArrayDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.StreamTableJoinFunction;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.remote.NoOpTableReadFunction;
import org.apache.samza.table.remote.couchbase.CouchbaseTableReadFunction;
import org.apache.samza.table.remote.couchbase.CouchbaseTableWriteFunction;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/**
 * This class does an end to end testing for Couchbase table functions. It puts some data to an in-memory stream and a
 * mocked Couchbase bucket. Then it does a join between the stream and the Couchbase table. Finally it writes the
 * result to another mocked Couchbase bucket and assert the result to be what we expect. Due to the problem of
 * CouchbaseMock library, closing a mocked bucket would throw exceptions like: java.lang.ArithmeticException: / by zero.
 * Please ignore those exception.
 */
public class TestCouchbaseRemoteTableEndToEnd {
  private CouchbaseEnvironment couchbaseEnvironment;
  private CouchbaseMock couchbaseMock;
  private Cluster cluster;
  private String inputBucketName = "inputBucket";
  private String outputBucketName = "outputBucket";

  private void createMockBuckets(List<String> bucketNames) throws Exception {
    ArrayList<BucketConfiguration> configList = new ArrayList<>();
    bucketNames.forEach(name -> configList.add(configBucket(name)));
    couchbaseMock = new CouchbaseMock(0, configList);
    couchbaseMock.start();
    couchbaseMock.waitForStartup();
  }

  private BucketConfiguration configBucket(String bucketName) {
    BucketConfiguration config = new BucketConfiguration();
    config.numNodes = 1;
    config.numReplicas = 1;
    config.name = bucketName;
    return config;
  }

  private void initClient() {
    couchbaseEnvironment = DefaultCouchbaseEnvironment.builder()
        .bootstrapCarrierDirectPort(couchbaseMock.getCarrierPort("inputBucket"))
        .bootstrapHttpDirectPort(couchbaseMock.getHttpPort())
        .build();
    cluster = CouchbaseCluster.create(couchbaseEnvironment, "couchbase://127.0.0.1");
  }

  @Before
  public void setup() throws Exception {
    List<String> bucketNames = new ArrayList<>();
    bucketNames.add(inputBucketName);
    bucketNames.add(outputBucketName);
    createMockBuckets(bucketNames);
    initClient();
  }

  @After
  public void shutdownMock() {
    couchbaseMock.stop();
  }

  @Test
  public void testEndToEnd() {
    Bucket inputBucket = cluster.openBucket(inputBucketName);
    inputBucket.upsert(ByteArrayDocument.create("Alice", "20".getBytes()));
    inputBucket.upsert(ByteArrayDocument.create("Bob", "30".getBytes()));
    inputBucket.upsert(ByteArrayDocument.create("Chris", "40".getBytes()));
    inputBucket.upsert(ByteArrayDocument.create("David", "50".getBytes()));
    inputBucket.close();

    List<String> users = Arrays.asList("Alice", "Bob", "Chris", "David");

    final StreamApplication app = appDesc -> {
      DelegatingSystemDescriptor inputSystemDescriptor = new DelegatingSystemDescriptor("test");
      GenericInputDescriptor<String> inputDescriptor =
          inputSystemDescriptor.getInputDescriptor("User", new NoOpSerde<>());

      CouchbaseTableReadFunction<String> readFunction = new CouchbaseTableReadFunction<>(inputBucketName,
              String.class, "couchbase://127.0.0.1")
          .withBootstrapCarrierDirectPort(couchbaseMock.getCarrierPort(inputBucketName))
          .withBootstrapHttpDirectPort(couchbaseMock.getHttpPort())
          .withSerde(new StringSerde());

      CouchbaseTableWriteFunction<JsonObject> writeFunction = new CouchbaseTableWriteFunction<>(outputBucketName,
              JsonObject.class, "couchbase://127.0.0.1")
          .withBootstrapCarrierDirectPort(couchbaseMock.getCarrierPort(outputBucketName))
          .withBootstrapHttpDirectPort(couchbaseMock.getHttpPort());

      RemoteTableDescriptor inputTableDesc = new RemoteTableDescriptor<String, String>("input-table")
          .withReadFunction(readFunction)
          .withRateLimiterDisabled();
      Table<KV<String, String>> inputTable = appDesc.getTable(inputTableDesc);

      RemoteTableDescriptor outputTableDesc = new RemoteTableDescriptor<String, JsonObject>("output-table")
          .withReadFunction(new NoOpTableReadFunction<>())
          .withWriteFunction(writeFunction)
          .withRateLimiterDisabled();
      Table<KV<String, JsonObject>> outputTable = appDesc.getTable(outputTableDesc);

      appDesc.getInputStream(inputDescriptor)
          .map(k -> KV.of(k, k))
          .join(inputTable, new JoinFunction())
          .sendTo(outputTable);
    };

    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    InMemoryInputDescriptor<TestTableData.PageView> inputDescriptor = isd
        .getInputDescriptor("User", new NoOpSerde<>());
    TestRunner.of(app)
        .addInputStream(inputDescriptor, users)
        .run(Duration.ofSeconds(10));

    Bucket outputBucket = cluster.openBucket(outputBucketName);
    Assert.assertEquals("{\"name\":\"Alice\",\"age\":\"20\"}", outputBucket.get("Alice").content().toString());
    Assert.assertEquals("{\"name\":\"Bob\",\"age\":\"30\"}", outputBucket.get("Bob").content().toString());
    Assert.assertEquals("{\"name\":\"Chris\",\"age\":\"40\"}", outputBucket.get("Chris").content().toString());
    Assert.assertEquals("{\"name\":\"David\",\"age\":\"50\"}", outputBucket.get("David").content().toString());
    outputBucket.close();
  }

  static class JoinFunction
      implements StreamTableJoinFunction<String, KV<String, String>, KV<String, String>, KV<String, JsonObject>> {

    @Override
    public KV<String, JsonObject> apply(KV<String, String> message, KV<String, String> record) {
      return KV.of(message.getKey(), JsonObject.create().put("name", message.key).put("age", record.getValue()));
    }

    @Override
    public String getMessageKey(KV<String, String> message) {
      return message.getKey();
    }

    @Override
    public String getRecordKey(KV<String, String> record) {
      return record.getKey();
    }
  }
}