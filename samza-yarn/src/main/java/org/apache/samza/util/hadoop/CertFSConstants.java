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

package org.apache.samza.util.hadoop;

public class CertFSConstants {

  /**
   * URI Scheme for certfs://host/path URIs.
   */
  public static final String CERTFS_URI_SCHEME = "certfs";

  /**
   * URI Scheme for https://host/path URIs.
   */
  public static final String HTTPS_URI_SCHEME = "https";

  public static final int HTTP_STATUS_OK = 200;

  public static final int DEFAULT_BLOCK_SIZE = 4096; // 4 * 1024;

  public static final String SIGNATURE_ALGORITHM = "SHA512withRSA";
  public static final String KEY_ALGORITHM = "RSA";
  public static final int KEY_SIZE = 2048;

  public static final String TRUSTSTORE_TYPE = "csr.ssl.truststore.type";
  public static final String TRUSTSTORE_LOCATION = "csr.ssl.truststore.location";
  public static final String TRUSTSTORE_PASSWORD = "csr.ssl.truststore.password";
  public static final String KEYSTORE_TYPE = "csr.ssl.keystore.type";
  public static final String KEYSTORE_LOCATION = "csr.ssl.keystore.location";
  public static final String KEYSTORE_PASSWORD = "csr.ssl.keystore.password";
  public static final String KEY_PASSWORD = "csr.ssl.key.password";
  public static final String BCSTYLE_X500NAME = "csr.ssl.bcstyle.x500name";


  public static final String HOSTNAME_PLACEHOLDER = "__HOSTNAME__";

  public static final String YARN_CONFIG_FS_CERTFS_IMPL = "fs.certfs.impl";
  public static final String SAMZA_CONFIG_FS_CERTFS_IMPL_OVERRIDE = "fs.certfs.impl.override";
}
