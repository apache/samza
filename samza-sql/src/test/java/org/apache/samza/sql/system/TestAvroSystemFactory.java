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

import java.nio.ByteBuffer;
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
import org.apache.samza.sql.avro.schemas.MyFixed;
import org.apache.samza.sql.avro.schemas.PageView;
import org.apache.samza.sql.avro.schemas.PhoneNumber;
import org.apache.samza.sql.avro.schemas.Profile;
import org.apache.samza.sql.avro.schemas.SimpleRecord;
import org.apache.samza.sql.avro.schemas.StreetNumRecord;
import org.apache.samza.sql.avro.schemas.SubRecord;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestAvroSystemFactory implements SystemFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TestAvroSystemFactory.class);

  public static final String CFG_NUM_MESSAGES = "numMessages";
  public static final String CFG_INCLUDE_NULL_FOREIGN_KEYS = "includeNullForeignKeys";
  public static final String CFG_INCLUDE_NULL_SIMPLE_RECORDS = "includeNullSimpleRecords";
  public static final String CFG_SLEEP_BETWEEN_POLLS_MS = "sleepBetweenPollsMs";

  private static final String[] PROFILE_NAMES = {"John", "Mike", "Mary", "Joe", "Brad", "Jennifer"};
  private static final int[] PROFILE_ZIPS = {94000, 94001, 94002, 94003, 94004, 94005};
  private static final int[] STREET_NUMS = {1234, 1235, 1236, 1237, 1238, 1239};
  private static final String[] PHONE_NUMBERS = {"000-000-0000", "111-111-1111", "222-222-2222", "333-333-3333",
      "444-444-4444", "555-555-5555"};
  public static final String[] COMPANIES = {"MSFT", "LKND", "GOOG", "FB", "AMZN", "CSCO"};
  public static final String[] PAGE_KEYS = {"inbox", "home", "search", "pymk", "group", "job"};
  public static final byte[] DEFAULT_TRACKING_ID_BYTES =
    {76, 75, -24, 10, 33, -117, 24, -52, -110, -39, -5, 102, 65, 57, -62, -1};
  public static final int NULL_RECORD_FREQUENCY = 5;


  public static volatile List<OutgoingMessageEnvelope> messages = new ArrayList<>();

  public static List<String> getPageKeyProfileNameJoin(int numMessages) {
    return IntStream.range(0, numMessages)
                .mapToObj(i -> PAGE_KEYS[i % PAGE_KEYS.length] + "," + PROFILE_NAMES[i % PROFILE_NAMES.length])
                .collect(Collectors.toList());
  }

  public static List<String> getPageKeyProfileNameAddressJoin(int numMessages) {
    return IntStream.range(0, numMessages)
        .mapToObj(i -> PAGE_KEYS[i % PAGE_KEYS.length] + "," + PROFILE_NAMES[i % PROFILE_NAMES.length] + "," +
            PROFILE_ZIPS[i % PROFILE_ZIPS.length] + "," + STREET_NUMS[i % STREET_NUMS.length])
        .collect(Collectors.toList());
  }

  public static List<String> getPageKeyProfileNameJoinWithNullForeignKeys(int numMessages) {
    // All even profileId foreign keys are null
    return IntStream.range(0, numMessages / 2)
        .mapToObj(i -> PAGE_KEYS[(i * 2 + 1) % PAGE_KEYS.length] + "," + PROFILE_NAMES[(i * 2 + 1) % PROFILE_NAMES.length])
        .collect(Collectors.toList());
  }

  public static List<String> getPageKeyProfileNameOuterJoinWithNullForeignKeys(int numMessages) {
    // All even profileId foreign keys are null
    return IntStream.range(0, numMessages)
        .mapToObj(i -> PAGE_KEYS[i % PAGE_KEYS.length] + "," + ((i % 2 == 0) ? "null" : PROFILE_NAMES[i % PROFILE_NAMES.length]))
        .collect(Collectors.toList());
  }

  public static List<String> getPageKeyProfileCompanyNameJoin(int numMessages) {
    return IntStream.range(0, numMessages)
        .mapToObj(i -> PAGE_KEYS[i % PAGE_KEYS.length] + "," + PROFILE_NAMES[i % PROFILE_NAMES.length] +
            "," + COMPANIES[i % COMPANIES.length])
        .collect(Collectors.toList());
  }

  public static HashMap<String, Integer> getPageKeyGroupByResult(int numMessages, Set<String> includePageKeys) {
    HashMap<String, Integer> pageKeyCountMap = new HashMap<>();
    int quotient = numMessages / PAGE_KEYS.length;
    int remainder = numMessages % PAGE_KEYS.length;
    IntStream.range(0, PAGE_KEYS.length)
        .map(k -> {
          if (includePageKeys.contains(PAGE_KEYS[k])) {
            pageKeyCountMap.put(PAGE_KEYS[k], quotient + ((k < remainder) ? 1 : 0));
          }
          return k;
        });
    return pageKeyCountMap;
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
    private final boolean includeNullSimpleRecords;
    private final long sleepBetweenPollsMs;
    private final Set<SystemStreamPartition> simpleRecordSsps = new HashSet<>();
    private final Set<SystemStreamPartition> profileRecordSsps = new HashSet<>();
    private final Set<SystemStreamPartition> companyRecordSsps = new HashSet<>();
    private final Set<SystemStreamPartition> pageViewRecordSsps = new HashSet<>();
    private final Map<SystemStreamPartition, Integer> curMessagesPerSsp = new HashMap<>();

    public TestAvroSystemConsumer(String systemName, Config config) {
      numMessages = config.getInt(String.format("systems.%s.%s", systemName, CFG_NUM_MESSAGES), DEFAULT_NUM_EVENTS);
      includeNullForeignKeys = config.getBoolean(String.format("systems.%s.%s", systemName,
          CFG_INCLUDE_NULL_FOREIGN_KEYS), false);
      includeNullSimpleRecords = config.getBoolean(String.format("systems.%s.%s", systemName,
          CFG_INCLUDE_NULL_SIMPLE_RECORDS), false);
      sleepBetweenPollsMs = config.getLong(String.format("systems.%s.%s", systemName, CFG_SLEEP_BETWEEN_POLLS_MS), 0);
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition, String offset) {
      switch (systemStreamPartition.getStream().toLowerCase()) {
        case "simple1":
          simpleRecordSsps.add(systemStreamPartition);
          break;
        case "profile":
          profileRecordSsps.add(systemStreamPartition);
          break;
        case "company":
          companyRecordSsps.add(systemStreamPartition);
          break;
        case "pageview":
          pageViewRecordSsps.add(systemStreamPartition);
          break;
        case "complex1":
          break;
        case "simple2":
          break;
        case "simple3":
          break;
        default:
          Assert.assertTrue(String.format("ssp %s is not recognized", systemStreamPartition), false);
          break;
      }
      curMessagesPerSsp.put(systemStreamPartition, 0);
    }

    @Override
    public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> set, long timeout)
        throws InterruptedException {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> envelopeMap = new HashMap<>();
      set.forEach(ssp -> {
        int curMessages = curMessagesPerSsp.get(ssp);
        // We send num Messages and an end of stream message following that.
        List<IncomingMessageEnvelope> envelopes =
            IntStream.range(curMessages, curMessages + numMessages / 4)
                .mapToObj(i -> i < numMessages ? new IncomingMessageEnvelope(ssp, null, getKey(i, ssp),
                    getData(i, ssp)) : IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp))
                .collect(Collectors.toList());
        envelopeMap.put(ssp, envelopes);
        curMessagesPerSsp.put(ssp, curMessages + numMessages / 4);
      });
      if (sleepBetweenPollsMs > 0) {
        Thread.sleep(sleepBetweenPollsMs);
      }

      return envelopeMap;
    }

    private Object getKey(int index, SystemStreamPartition ssp) {
      if (profileRecordSsps.contains(ssp) || companyRecordSsps.contains(ssp)) {
        return index; // Keep this value the same as the profile/company record's "id" field.
      }
      return "key" + index;
    }

    private Object getData(int index, SystemStreamPartition ssp) {
      if (simpleRecordSsps.contains(ssp)) {
        return createSimpleRecord(index, includeNullSimpleRecords);
      } else if (profileRecordSsps.contains(ssp)) {
        return createProfileRecord(index);
      } else if (companyRecordSsps.contains(ssp)) {
        return createCompanyRecord(index);
      } else if (pageViewRecordSsps.contains(ssp)) {
        return createPageViewRecord(index);
      } else {
        return createComplexRecord(index);
      }
    }

    private Object createSimpleRecord(int index, boolean includeNullRecords) {
      if (includeNullRecords && index % NULL_RECORD_FREQUENCY == 0) {
        return null;
      }

      GenericRecord record = new GenericData.Record(SimpleRecord.SCHEMA$);
      record.put("id", index);
      record.put("name", "Name" + index);
      return record;
    }

    private Object createProfileRecord(int index) {
      GenericRecord record = new GenericData.Record(Profile.SCHEMA$);
      record.put("id", index);
      record.put("name", PROFILE_NAMES[index % PROFILE_NAMES.length]);
      record.put("address", createProfileAddressRecord(index));
      record.put("companyId", includeNullForeignKeys && (index % 2 == 0) ? null : index % COMPANIES.length);
      record.put("phoneNumbers", createProfilePhoneNumbers(index % PHONE_NUMBERS.length));
      return record;
    }

    private Object createProfileAddressRecord(int index) {
      GenericRecord record = new GenericData.Record(AddressRecord.SCHEMA$);
      record.put("streetnum", createProfileStreetNumRecord(index));
      record.put("zip", PROFILE_ZIPS[index % PROFILE_NAMES.length]);
      return record;
    }

    private Object createProfileStreetNumRecord(int index) {
      GenericRecord record = new GenericData.Record(StreetNumRecord.SCHEMA$);
      record.put("number", STREET_NUMS[index % STREET_NUMS.length]);
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
      StringBuilder number = new StringBuilder(PHONE_NUMBERS[index]);
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
      record.put("name", COMPANIES[index % COMPANIES.length]);
      return record;
    }

    private Object createPageViewRecord(int index) {
      GenericRecord record = new GenericData.Record(PageView.SCHEMA$);
      // All even profileId foreign keys are null
      record.put("profileId", includeNullForeignKeys && (index % 2 == 0) ? null : index);
      record.put("pageKey", PAGE_KEYS[index % PAGE_KEYS.length]);
      return record;
    }

    private Object createComplexRecord(int index) {

      GenericRecord record = new GenericData.Record(ComplexRecord.SCHEMA$);
      record.put("id", index);
      record.put("string_value", "Name" + index);
      record.put("bytes_value", ByteBuffer.wrap("sample bytes".getBytes()));
      record.put("float_value0", index + 0.123456f);
      record.put("double_value", index + 0.0123456789);
      MyFixed myFixedVar = new MyFixed();
      myFixedVar.bytes(DEFAULT_TRACKING_ID_BYTES);
      record.put("fixed_value", myFixedVar);
      record.put("bool_value", index % 2 == 0);
      GenericData.Array<String> arrayValues =
          new GenericData.Array<>(index, ComplexRecord.SCHEMA$.getField("array_values").schema().getTypes().get(1));
      arrayValues.addAll(IntStream.range(0, index).mapToObj(String::valueOf).collect(Collectors.toList()));
      record.put("array_values", arrayValues);
//      record.put("union_value", "unionStrValue");
      GenericRecord subRecord = new GenericData.Record(SubRecord.SCHEMA$);
      subRecord.put("id", index);
      subRecord.put("sub_values", arrayValues);
      record.put("union_value", subRecord);
      Map<String, String> mapValues = new HashMap<>();
      mapValues.put("key0", "value0");
      record.put("map_values", mapValues);
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
