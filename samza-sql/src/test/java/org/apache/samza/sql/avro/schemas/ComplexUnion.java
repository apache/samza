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
package org.apache.samza.sql.avro.schemas;
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class ComplexUnion extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ComplexUnion\",\"namespace\":\"org.apache.samza.sql.avro.schemas\",\"fields\":[{\"name\":\"non_nullable_union_value\",\"type\":[\"int\",\"string\"],\"doc\":\"union Value.\"}],\"version\":1}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  /** union Value. */
  @Deprecated public java.lang.Object non_nullable_union_value;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public ComplexUnion() {}

  /**
   * All-args constructor.
   */
  public ComplexUnion(java.lang.Object non_nullable_union_value) {
    this.non_nullable_union_value = non_nullable_union_value;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return non_nullable_union_value;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: non_nullable_union_value = (java.lang.Object)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'non_nullable_union_value' field.
   * union Value.   */
  public java.lang.Object getNonNullableUnionValue() {
    return non_nullable_union_value;
  }

  /**
   * Sets the value of the 'non_nullable_union_value' field.
   * union Value.   * @param value the value to set.
   */
  public void setNonNullableUnionValue(java.lang.Object value) {
    this.non_nullable_union_value = value;
  }

  /** Creates a new ComplexUnion RecordBuilder */
  public static org.apache.samza.sql.avro.schemas.ComplexUnion.Builder newBuilder() {
    return new org.apache.samza.sql.avro.schemas.ComplexUnion.Builder();
  }

  /** Creates a new ComplexUnion RecordBuilder by copying an existing Builder */
  public static org.apache.samza.sql.avro.schemas.ComplexUnion.Builder newBuilder(org.apache.samza.sql.avro.schemas.ComplexUnion.Builder other) {
    return new org.apache.samza.sql.avro.schemas.ComplexUnion.Builder(other);
  }

  /** Creates a new ComplexUnion RecordBuilder by copying an existing ComplexUnion instance */
  public static org.apache.samza.sql.avro.schemas.ComplexUnion.Builder newBuilder(org.apache.samza.sql.avro.schemas.ComplexUnion other) {
    return new org.apache.samza.sql.avro.schemas.ComplexUnion.Builder(other);
  }

  /**
   * RecordBuilder for ComplexUnion instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ComplexUnion>
    implements org.apache.avro.data.RecordBuilder<ComplexUnion> {

    private java.lang.Object non_nullable_union_value;

    /** Creates a new Builder */
    private Builder() {
      super(org.apache.samza.sql.avro.schemas.ComplexUnion.SCHEMA$);
    }

    /** Creates a Builder by copying an existing Builder */
    private Builder(org.apache.samza.sql.avro.schemas.ComplexUnion.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.non_nullable_union_value)) {
        this.non_nullable_union_value = data().deepCopy(fields()[0].schema(), other.non_nullable_union_value);
        fieldSetFlags()[0] = true;
      }
    }

    /** Creates a Builder by copying an existing ComplexUnion instance */
    private Builder(org.apache.samza.sql.avro.schemas.ComplexUnion other) {
            super(org.apache.samza.sql.avro.schemas.ComplexUnion.SCHEMA$);
      if (isValidValue(fields()[0], other.non_nullable_union_value)) {
        this.non_nullable_union_value = data().deepCopy(fields()[0].schema(), other.non_nullable_union_value);
        fieldSetFlags()[0] = true;
      }
    }

    /** Gets the value of the 'non_nullable_union_value' field */
    public java.lang.Object getNonNullableUnionValue() {
      return non_nullable_union_value;
    }

    /** Sets the value of the 'non_nullable_union_value' field */
    public org.apache.samza.sql.avro.schemas.ComplexUnion.Builder setNonNullableUnionValue(java.lang.Object value) {
      validate(fields()[0], value);
      this.non_nullable_union_value = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /** Checks whether the 'non_nullable_union_value' field has been set */
    public boolean hasNonNullableUnionValue() {
      return fieldSetFlags()[0];
    }

    /** Clears the value of the 'non_nullable_union_value' field */
    public org.apache.samza.sql.avro.schemas.ComplexUnion.Builder clearNonNullableUnionValue() {
      non_nullable_union_value = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    public ComplexUnion build() {
      try {
        ComplexUnion record = new ComplexUnion();
        record.non_nullable_union_value = fieldSetFlags()[0] ? this.non_nullable_union_value : (java.lang.Object) defaultValue(fields()[0]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
