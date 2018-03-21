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

package org.apache.samza.sql.system;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.sql.avro.schemas.Company;
import org.apache.samza.sql.avro.schemas.ComplexRecord;
import org.apache.samza.sql.avro.schemas.PageView;
import org.apache.samza.sql.avro.schemas.Profile;
import org.apache.samza.sql.avro.schemas.SimpleRecord;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestAvroSystemFactory implements SystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TestAvroSystemFactory.class);

  public static final String CFG_NUM_MESSAGES = "numMessages";
  public static final String CFG_INCLUDE_NULL_FOREIGN_KEYS = "includeNullForeignKeys";
  public static List<OutgoingMessageEnvelope> messages = new ArrayList<>();

  public static final String[] profiles = {"John", "Mike", "Mary", "Joe", "Brad", "Jennifer"};
  public static final String[] companies = {"MSFT", "LKND", "GOOG", "FB", "AMZN", "CSCO"};
  public static final String[] pagekeys = {"inbox", "home", "search", "pymk", "group", "job"};

  public static List<String> getPageKeyProfileNameJoin(int numMessages) {
    return IntStream.range(0, numMessages)
                .mapToObj(i -> pagekeys[i % pagekeys.length] + "," + profiles[i % profiles.length])
                .collect(Collectors.toList());
  }

  public static List<String> getPageKeyProfileNameJoinWithNullForeignKeys(int numMessages) {
    // All even profileId foreign keys are null
    return IntStream.range(0, numMessages / 2)
        .mapToObj(i -> pagekeys[(i * 2 + 1) % pagekeys.length] + "," + profiles[(i * 2 + 1) % profiles.length])
        .collect(Collectors.toList());
  }

  public static List<String> getPageKeyProfileNameOuterJoinWithNullForeignKeys(int numMessages) {
    // All even profileId foreign keys are null
    return IntStream.range(0, numMessages)
        .mapToObj(i -> pagekeys[i % pagekeys.length] + "," + ((i % 2 == 0) ? "null" : profiles[i % profiles.length]))
        .collect(Collectors.toList());
  }

  public static List<String> getPageKeyProfileCompanyNameJoin(int numMessages) {
    return IntStream.range(0, numMessages)
        .mapToObj(i -> pagekeys[i % pagekeys.length] + "," + profiles[i % profiles.length] +
            "," + companies[i % companies.length])
        .collect(Collectors.toList());
  }

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    return new TestAvroSystemConsumer(systemName, config);
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new TestAvroSystemProducer();
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new SimpleSystemAdmin(config);
  }

  private class TestAvroSystemConsumer implements SystemConsumer {
    public static final int DEFAULT_NUM_EVENTS = 10;
    private final int numMessages;
    private final boolean includeNullForeignKeys;
    private final Set<SystemStreamPartition> simpleRecordMap = new HashSet<>();
    private final Set<SystemStreamPartition> profileRecordMap = new HashSet<>();
    private final Set<SystemStreamPartition> companyRecordMap = new HashSet<>();
    private final Set<SystemStreamPartition> pageViewRecordMap = new HashSet<>();

    public TestAvroSystemConsumer(String systemName, Config config) {
      numMessages = config.getInt(String.format("systems.%s.%s", systemName, CFG_NUM_MESSAGES), DEFAULT_NUM_EVENTS);
      includeNullForeignKeys = config.getBoolean(String.format("systems.%s.%s", systemName,
          CFG_INCLUDE_NULL_FOREIGN_KEYS), false);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition, String offset) {
      if (systemStreamPartition.getStream().toLowerCase().contains("simple1")) {
        simpleRecordMap.add(systemStreamPartition);
      }
      if (systemStreamPartition.getStream().toLowerCase().contains("profile")) {
        profileRecordMap.add(systemStreamPartition);
      }
      if (systemStreamPartition.getStream().toLowerCase().contains("company")) {
        companyRecordMap.add(systemStreamPartition);
      }
      if (systemStreamPartition.getStream().toLowerCase().contains("pageview")) {
        pageViewRecordMap.add(systemStreamPartition);
      }
    }

    @Override
    public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> set, long timeout)
        throws InterruptedException {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> envelopeMap = new HashMap<>();
      set.forEach(ssp -> {
        // We send num Messages and an end of stream message following that.
        List<IncomingMessageEnvelope> envelopes = IntStream.range(0, numMessages + 1)
            .mapToObj(i -> i < numMessages ? new IncomingMessageEnvelope(ssp, null, "key" + i,
                getData(i, ssp)) : IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp))
            .collect(Collectors.toList());
        envelopeMap.put(ssp, envelopes);
      });

      return envelopeMap;
    }

    private Object getData(int index, SystemStreamPartition ssp) {
      if (simpleRecordMap.contains(ssp)) {
        return createSimpleRecord(index);
      } else if (profileRecordMap.contains(ssp)) {
        return createProfileRecord(index);
      } else if (companyRecordMap.contains(ssp)) {
        return createCompanyRecord(index);
      } else if (pageViewRecordMap.contains(ssp)) {
        return createPageViewRecord(index);
      } else {
        return createComplexRecord(index);
      }
    }

    private Object createSimpleRecord(int index) {
      GenericRecord record = new GenericData.Record(SimpleRecord.SCHEMA$);
      record.put("id", index);
      record.put("name", "Name" + index);
      return record;
    }

    private Object createProfileRecord(int index) {
      GenericRecord record = new GenericData.Record(Profile.SCHEMA$);
      record.put("id", index);
      record.put("name", profiles[index % profiles.length]);
      record.put("companyId", includeNullForeignKeys && (index % 2 == 0) ? null : index % companies.length);
      return record;
    }

    private Object createCompanyRecord(int index) {
      GenericRecord record = new GenericData.Record(Company.SCHEMA$);
      record.put("id", index);
      record.put("name", companies[index % companies.length]);
      return record;
    }

    private Object createPageViewRecord(int index) {
      GenericRecord record = new GenericData.Record(PageView.SCHEMA$);
      // All even profileId foreign keys are null
      record.put("profileId", includeNullForeignKeys && (index % 2 == 0) ? null : index);
      record.put("pageKey", pagekeys[index % pagekeys.length]);
      return record;
    }

    private Object createComplexRecord(int index) {
      GenericRecord record = new GenericData.Record(ComplexRecord.SCHEMA$);
      record.put("id", index);
      record.put("string_value", "Name" + index);
      GenericData.Array<String> arrayValues =
          new GenericData.Array<>(index, ComplexRecord.SCHEMA$.getField("array_values").schema().getTypes().get(1));
      arrayValues.addAll(IntStream.range(0, index).mapToObj(String::valueOf).collect(Collectors.toList()));
      record.put("array_values", arrayValues);
      return record;
    }
  }

  private class TestAvroSystemProducer implements SystemProducer {

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void register(String source) {
    }

    @Override
    public void send(String source, OutgoingMessageEnvelope envelope) {
      LOG.info("Adding message " + envelope);
      messages.add(envelope);
    }

    @Override
    public void flush(String source) {
    }
  }
}
