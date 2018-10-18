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

package org.apache.samza.sql.client.impl;

import com.google.common.base.Joiner;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.samza.sql.client.interfaces.SqlFunction;

/**
 * UDF information displayer
 */
public class SamzaSqlUdfDisplayInfo implements SqlFunction {

    private String name;

    private String description;

    private List<SamzaSqlFieldType> argumentTypes;

    private SamzaSqlFieldType returnType;

    public SamzaSqlUdfDisplayInfo(String name, String description, List<SamzaSqlFieldType> argumentTypes,
        SamzaSqlFieldType returnType) {
        this.name = name;
        this.description = description;
        this.argumentTypes = argumentTypes;
        this.returnType = returnType;
    }

  public String getName() {
      return name;
  }

  public String getDescription() {
      return description;
  }

  public List<String> getArgumentTypes() {
      return argumentTypes.stream().map(x -> x.getTypeName().toString()).collect(Collectors.toList());
  }

  public String getReturnType() {
      return returnType.getTypeName().toString();
  }

  public String toString() {
      List<String> argumentTypeNames =
          argumentTypes.stream().map(x -> x.getTypeName().toString()).collect(Collectors.toList());
      String args = Joiner.on(", ").join(argumentTypeNames);
      return String.format("%s(%s) returns <%s> : %s", name, args, returnType.getTypeName().toString(), description);
  }
}
