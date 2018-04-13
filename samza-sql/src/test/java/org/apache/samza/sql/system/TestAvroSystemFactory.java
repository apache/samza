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
import org.apache.samza.sql.avro.schemas.AddressRecord;
import org.apache.samza.sql.avro.schemas.Company;
import org.apache.samza.sql.avro.schemas.ComplexRecord;
import org.apache.samza.sql.avro.schemas.Kind;
import org.apache.samza.sql.avro.schemas.PageView;
import org.apache.samza.sql.avro.schemas.PhoneNumber;
import org.apache.samza.sql.avro.schemas.Profile;
import org.apache.samza.sql.avro.schemas.SimpleRecord;
import org.apache.samza.sql.avro.schemas.StreetNumRecord;
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

  private static final String[] profileNames = {"John", "Mike", "Mary", "Joe", "Brad", "Jennifer"};
  private static final int[] profileZips = {94000, 94001, 94002, 94003, 94004, 94005};
  private static final int[] streetNums = {1234, 1235, 1236, 1237, 1238, 1239};
  private static final String[] phoneNumbers = {"000-000-0000", "111-111-1111", "222-222-2222", "333-333-3333",
      "444-444-4444", "555-555-5555"};
  public static final String[] companies = {"MSFT", "LKND", "GOOG", "FB", "AMZN", "CSCO"};
  private static final String[] pagekeys = {"inbox", "home", "search", "pymk", "group", "job"};

  public static List<String> getPageKeyProfileNameJoin(int numMessages) {
    return IntStream.range(0, numMessages)
                .mapToObj(i -> pagekeys[i % pagekeys.length] + "," + profileNames[i % profileNames.length])
                .collect(Collectors.toList());
  }

  public static List<String> getPageKeyProfileNameAddressJoin(int numMessages) {
    return IntStream.range(0, numMessages)
        .mapToObj(i -> pagekeys[i % pagekeys.length] + "," + profileNames[i % profileNames.length] + "," +
            profileZips[i % profileZips.length] + "," + streetNums[i % streetNums.length])
        .collect(Collectors.toList());
  }

  public static List<String> getPageKeyProfileNameJoinWithNullForeignKeys(int numMessages) {
    // All even profileId foreign keys are null
    return IntStream.range(0, numMessages / 2)
        .mapToObj(i -> pagekeys[(i * 2 + 1) % pagekeys.length] + "," + profileNames[(i * 2 + 1) % profileNames.length])
        .collect(Collectors.toList());
  }

  public static List<String> getPageKeyProfileNameOuterJoinWithNullForeignKeys(int numMessages) {
    // All even profileId foreign keys are null
    return IntStream.range(0, numMessages)
        .mapToObj(i -> pagekeys[i % pagekeys.length] + "," + ((i % 2 == 0) ? "null" : profileNames[i % profileNames.length]))
        .collect(Collectors.toList());
  }

  public static List<String> getPageKeyProfileCompanyNameJoin(int numMessages) {
    return IntStream.range(0, numMessages)
        .mapToObj(i -> pagekeys[i % pagekeys.length] + "," + profileNames[i % profileNames.length] +
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
      record.put("name", profileNames[index % profileNames.length]);
      record.put("address", createProfileAddressRecord(index));
      record.put("companyId", includeNullForeignKeys && (index % 2 == 0) ? null : index % companies.length);
      record.put("phoneNumbers", createProfilePhoneNumbers(index % phoneNumbers.length));
      return record;
    }

    private Object createProfileAddressRecord(int index) {
      GenericRecord record = new GenericData.Record(AddressRecord.SCHEMA$);
      record.put("streetnum", createProfileStreetNumRecord(index));
      record.put("zip", profileZips[index % profileNames.length]);
      return record;
    }

    private Object createProfileStreetNumRecord(int index) {
      GenericRecord record = new GenericData.Record(StreetNumRecord.SCHEMA$);
      record.put("number", streetNums[index % streetNums.length]);
      return record;
    }

    private List<Object> createProfilePhoneNumbers(int index) {
      List<Object> phoneNums = new ArrayList<>();
      phoneNums.add(createPhoneNumberRecord(index, Kind.Home));
      phoneNums.add(createPhoneNumberRecord(index, Kind.Work));
      phoneNums.add(createPhoneNumberRecord(index, Kind.Cell));
      return phoneNums;
    }

    private Object createPhoneNumberRecord(int index, Kind kind) {
      GenericRecord record = new GenericData.Record(PhoneNumber.SCHEMA$);
      StringBuilder number = new StringBuilder(phoneNumbers[index]);
      int lastCharIdx = number.length() - 1;
      String suffix = "";
      switch (kind) {
        case Home:
          suffix = "1";
          break;
        case Work:
          suffix = "2";
          break;
        case Cell:
          suffix = "3";
          break;
      }
      number.replace(lastCharIdx, lastCharIdx + 1, suffix);
      record.put("number", number);
      record.put("kind", kind);
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
