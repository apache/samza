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

package org.apache.samza.sql.interfaces;

import java.lang.reflect.Method;
import java.util.List;
import com.google.common.base.Objects;
import org.apache.samza.config.Config;
import org.apache.samza.sql.schema.SamzaSqlFieldType;


/**
 * Metadata corresponding to the Udf
 */
public class UdfMetadata {

  private final String name;
  private final String displayName;

  private final String description;
  private final Method udfMethod;
  private final Config udfConfig;
  private final boolean disableArgCheck;
  private final List<SamzaSqlFieldType> arguments;

  private final SamzaSqlFieldType returnType;

  public UdfMetadata(String name, String description, Method udfMethod, Config udfConfig, List<SamzaSqlFieldType> arguments,
      SamzaSqlFieldType returnType, boolean disableArgCheck) {
    // Udfs are case insensitive
    this.name = name.toUpperCase();
    // Let's also store the original name for display purposes.
    this.displayName = name;
    this.description = description;
    this.udfMethod = udfMethod;
    this.udfConfig = udfConfig;
    this.arguments = arguments;
    this.returnType = returnType;
    this.disableArgCheck = disableArgCheck;
  }

  /**
   * @return returns the returnType of the Samza SQL UDF.
   */
  public SamzaSqlFieldType getReturnType() {
    return returnType;
  }

  public Config getUdfConfig() {
    return udfConfig;
  }

  /**
   * @return Returns the instance of the {@link Method} corresponding to the UDF.
   */
  public Method getUdfMethod() {
    return udfMethod;
  }

  /**
   * @return Returns the name of the Udf.
   */
  public String getName() {
    return name;
  }

  /**
   * @return Returns the name of the Udf for display purposes.
   */
  public String getDisplayName() {
    return displayName;
  }

  /**
   * @return Returns the description of the udf.
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return Returns the list of arguments that the udf should take.
   */
  public List<SamzaSqlFieldType> getArguments() {
    return arguments;
  }

  /**
   * @return Returns whether the argument check needs to be disabled.
   */
  public boolean isDisableArgCheck() {
    return disableArgCheck;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, udfMethod, arguments, returnType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof UdfMetadata)) return false;
    UdfMetadata that = (UdfMetadata) o;
    return Objects.equal(name, that.name) &&
            Objects.equal(udfMethod, that.udfMethod) &&
            Objects.equal(arguments, that.arguments) &&
            returnType == that.returnType;
  }
}
