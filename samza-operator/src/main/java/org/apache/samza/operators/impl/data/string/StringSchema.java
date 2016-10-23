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

import org.apache.samza.operators.data.Data;
import org.apache.samza.operators.data.Schema;

import java.util.Map;

public class StringSchema implements Schema {
    private Type type = Type.STRING;

    @Override
    public Type getType() {
      return Type.STRING;
    }

    @Override
    public Schema getElementType() {
      throw new UnsupportedOperationException("Can't getElmentType with non-array schema: " + this.type);
    }

    @Override
    public Schema getValueType() {
        throw new UnsupportedOperationException("Can't getValueType with non-map schema: " + this.type);
    }

    @Override
    public Map<String, Schema> getFields() {
        throw new UnsupportedOperationException("Can't get field types with unknown schema type:" + this.type);
    }

    @Override
    public Schema getFieldType(String fldName) {
        throw new UnsupportedOperationException("Can't getFieldType with non-map/non-struct schema: " + this.type);
    }

    @Override
    public Data read(Object object) {
        return new StringData(object);
    }

    @Override
    public Data transform(Data inputData) {
        if (inputData.schema().getType() != this.type) {
            throw new IllegalArgumentException("Can't transform a mismatched primitive type. this type:" + this.type
                    + ", input type:" + inputData.schema().getType());
        }
        return inputData;
    }

    @Override
    public boolean equals(Schema other) {
        return other.getType() == this.type;
    }
}
