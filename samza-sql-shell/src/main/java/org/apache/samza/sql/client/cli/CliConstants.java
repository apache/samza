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

package org.apache.samza.sql.client.cli;

/**
 * Constant definitions for the shell.
 */
class CliConstants {
  public static final String APP_NAME = "Samza SQL Shell";
  public static final String WINDOW_TITLE = "Samza SQL Shell";
  public static final String PROMPT_1ST = "Samza SQL";
  public static final String PROMPT_1ST_END = "> ";

  // All shell environment variables starts with the prefix
  public static final String CONFIG_SHELL_PREFIX = "shell.";
  // Specifies the executor used by the shell
  public static final String CONFIG_EXECUTOR = "shell.executor";
  public static final String DEFAULT_EXECUTOR_CLASS = "org.apache.samza.sql.client.impl.SamzaExecutor";

  public static final String WELCOME_MESSAGE;
  static {
        WELCOME_MESSAGE =
"      ___           ___           ___           ___           ___ \n" +
"     /  /\\         /  /\\         /  /\\         /__/\\         /  /\\ \n" +
"    /  /::\\       /  /::\\       /  /::|        \\  \\:\\       /  /::\\ \n"+
"   /__/:/\\:\\     /  /:/\\:\\     /  /:|:|         \\  \\:\\     /  /:/\\:\\ \n"+
"  _\\_ \\:\\ \\:\\   /  /::\\ \\:\\   /  /:/|:|__        \\  \\:\\   /  /::\\ \\:\\ \n"+
" /__/\\ \\:\\ \\:\\ /__/:/\\:\\_\\:\\ /__/:/_|::::\\  ______\\__\\:\\ /__/:/\\:\\_\\:\\ \n"+
" \\  \\:\\ \\:\\_\\/ \\__\\/  \\:\\/:/ \\__\\/  /~~/:/ \\  \\::::::::/ \\__\\/  \\:\\/:/ \n"+
"  \\  \\:\\_\\:\\        \\__\\::/        /  /:/   \\  \\:\\~~~~~       \\__\\::/ \n"+
"   \\  \\:\\/:/        /  /:/        /  /:/     \\  \\:\\           /  /:/ \n"+
"    \\  \\::/        /__/:/        /__/:/       \\  \\:\\         /__/:/ \n"+
"     \\__\\/         \\__\\/         \\__\\/         \\__\\/         \\__\\/  \n\n"+
"Welcome to Samza SQL shell. Enter HELP for all commands.\n";
  }

  public static final char SPACE = '\u0020';
}
