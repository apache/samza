/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.apache.samza.sql.avro.schemas;

@SuppressWarnings("all")
public class EnrichedPageView extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"EnrichedPageView\",\"namespace\":\"org.apache.samza.sql.avro.schemas\",\"fields\":[{\"name\":\"pageKey\",\"type\":[\"null\",\"string\"],\"doc\":\"Page key.\",\"default\":null},{\"name\":\"companyName\",\"type\":[\"null\",\"string\"],\"doc\":\"Company name.\",\"default\":null},{\"name\":\"profileName\",\"type\":[\"null\",\"string\"],\"doc\":\"Profile name.\",\"default\":null},{\"name\":\"profileAddress\",\"type\":{\"type\":\"record\",\"name\":\"AddressRecord\",\"fields\":[{\"name\":\"zip\",\"type\":[\"null\",\"int\"],\"doc\":\"zip code.\",\"default\":null},{\"name\":\"streetnum\",\"type\":{\"type\":\"record\",\"name\":\"StreetNumRecord\",\"fields\":[{\"name\":\"number\",\"type\":[\"null\",\"int\"],\"doc\":\"street number.\",\"default\":null}]},\"doc\":\"Street Number\",\"default\":null}]},\"doc\":\"Profile Address\",\"default\":null}]}");
  /** Page key. */
  public java.lang.CharSequence pageKey;
  /** Company name. */
  public java.lang.CharSequence companyName;
  /** Profile name. */
  public java.lang.CharSequence profileName;
  /** Profile Address */
  public org.apache.samza.sql.avro.schemas.AddressRecord profileAddress;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return pageKey;
    case 1: return companyName;
    case 2: return profileName;
    case 3: return profileAddress;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: pageKey = (java.lang.CharSequence)value$; break;
    case 1: companyName = (java.lang.CharSequence)value$; break;
    case 2: profileName = (java.lang.CharSequence)value$; break;
    case 3: profileAddress = (org.apache.samza.sql.avro.schemas.AddressRecord)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
