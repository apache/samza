/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.apache.samza.sql.avro.schemas;

@SuppressWarnings("all")
public class AddressRecord extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"AddressRecord\",\"namespace\":\"org.apache.samza.sql.avro.schemas\",\"fields\":[{\"name\":\"zip\",\"type\":[\"null\",\"int\"],\"doc\":\"zip code.\",\"default\":null},{\"name\":\"streetnum\",\"type\":{\"type\":\"record\",\"name\":\"StreetNumRecord\",\"fields\":[{\"name\":\"number\",\"type\":[\"null\",\"int\"],\"doc\":\"street number.\",\"default\":null}]},\"doc\":\"Street Number\",\"default\":null}]}");
  /** zip code. */
  public java.lang.Integer zip;
  /** Street Number */
  public org.apache.samza.sql.avro.schemas.StreetNumRecord streetnum;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return zip;
    case 1: return streetnum;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: zip = (java.lang.Integer)value$; break;
    case 1: streetnum = (org.apache.samza.sql.avro.schemas.StreetNumRecord)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
