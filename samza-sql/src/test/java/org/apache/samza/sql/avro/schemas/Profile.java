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

/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package org.apache.samza.sql.avro.schemas;

@SuppressWarnings("all")
public class Profile extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"Profile\",\"namespace\":\"org.apache.samza.sql.avro.schemas\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"int\"],\"doc\":\"Profile id.\",\"default\":null},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"doc\":\"Profile name.\",\"default\":null},{\"name\":\"companyId\",\"type\":[\"null\",\"int\"],\"doc\":\"Company id.\",\"default\":null},{\"name\":\"address\",\"type\":{\"type\":\"record\",\"name\":\"AddressRecord\",\"fields\":[{\"name\":\"zip\",\"type\":[\"null\",\"int\"],\"doc\":\"zip code.\",\"default\":null},{\"name\":\"streetnum\",\"type\":{\"type\":\"record\",\"name\":\"StreetNumRecord\",\"fields\":[{\"name\":\"number\",\"type\":[\"null\",\"int\"],\"doc\":\"street number.\",\"default\":null}]},\"doc\":\"Street Number\",\"default\":null}]},\"doc\":\"Profile Address\",\"default\":null},{\"name\":\"selfEmployed\",\"type\":[\"null\",\"boolean\"],\"doc\":\"Boolean Value.\",\"default\":null},{\"name\":\"phoneNumbers\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"PhoneNumber\",\"fields\":[{\"name\":\"kind\",\"type\":{\"type\":\"enum\",\"name\":\"Kind\",\"symbols\":[\"Home\",\"Work\",\"Cell\"]}},{\"name\":\"number\",\"type\":\"string\"}]}}],\"doc\":\"array values in the record.\",\"default\":null},{\"name\":\"map_values\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}],\"doc\":\"map values in the record.\",\"default\":null}]}");
  /** Profile id. */
  public java.lang.Integer id;
  /** Profile name. */
  public java.lang.CharSequence name;
  /** Company id. */
  public java.lang.Integer companyId;
  /** Profile Address */
  public org.apache.samza.sql.avro.schemas.AddressRecord address;
  /** Boolean Value. */
  public java.lang.Boolean selfEmployed;
  /** array values in the record. */
  public java.util.List<PhoneNumber> phoneNumbers;
  /** map values in the record. */
  public java.util.Map<java.lang.CharSequence,java.lang.CharSequence> map_values;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return id;
    case 1: return name;
    case 2: return companyId;
    case 3: return address;
    case 4: return selfEmployed;
    case 5: return phoneNumbers;
    case 6: return map_values;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: id = (java.lang.Integer)value$; break;
    case 1: name = (java.lang.CharSequence)value$; break;
    case 2: companyId = (java.lang.Integer)value$; break;
    case 3: address = (org.apache.samza.sql.avro.schemas.AddressRecord)value$; break;
    case 4: selfEmployed = (java.lang.Boolean)value$; break;
    case 5: phoneNumbers = (java.util.List<PhoneNumber>)value$; break;
    case 6: map_values = (java.util.Map<java.lang.CharSequence,java.lang.CharSequence>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
