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

package org.apache.samza.operators.impl.data.string;

import org.apache.samza.operators.api.data.Data;
import org.apache.samza.operators.api.data.Schema;

import java.util.List;
import java.util.Map;

public class StringData implements Data {
    private final Object datum;
    private final Schema schema;

    public StringData(Object datum) {
        this.datum = datum;
        this.schema = new StringSchema();
    }

    @Override
    public Schema schema() {
        return this.schema;
    }

    @Override
    public Object value() {
        return this.datum;
    }

    @Override
    public int intValue() {
        throw new UnsupportedOperationException("Can't get int value for a string type data");
    }

    @Override
    public long longValue() {
        throw new UnsupportedOperationException("Can't get long value for a string type data");
    }

    @Override
    public float floatValue() {
        throw new UnsupportedOperationException("Can't get float value for a string type data");
    }

    @Override
    public double doubleValue() {
        throw new UnsupportedOperationException("Can't get double value for a string type data");
    }

    @Override
    public boolean booleanValue() {
        throw new UnsupportedOperationException("Can't get boolean value for a string type data");
    }

    @Override
    public String strValue() {
        return String.valueOf(datum);
    }

    @Override
    public byte[] bytesValue() {
        throw new UnsupportedOperationException("Can't get bytesValue for a string type data");
    }

    @Override
    public List<Object> arrayValue() {
        throw new UnsupportedOperationException("Can't get arrayValue for a string type data");
    }

    @Override
    public Map<Object, Object> mapValue() {
        throw new UnsupportedOperationException("Can't get mapValue for a string type data");
    }

    @Override
    public Data getElement(int index) {
        throw new UnsupportedOperationException("Can't getElement(index) on a string type data");
    }

    @Override
    public Data getFieldData(String fldName) {
        throw new UnsupportedOperationException("Can't getFieldData(fieldName) for a string type data");
    }
}
