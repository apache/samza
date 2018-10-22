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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

/**
 * CliEnvironment contains "environment variables" that configures the shell behavior.
 */
public class CliEnvironment {
  private static final String debugEnvVar = "shell.debug";
  private static PrintStream stdout = System.out;
  private static PrintStream stderr = System.err;
  private Boolean debug = false;

  boolean isDebug() {
    return debug;
  }

  void setDebug(Boolean debug) {
    this.debug = debug;
  }

  /**
   * @param var Environment variable
   * @param val Value of the environment variable
   * @return 0 : succeed
   * -1: invalid var
   * -2: invalid val
   */
  int setEnvironmentVariable(String var, String val) {
    switch (var.toLowerCase()) {
      case debugEnvVar:
        val = val.toLowerCase();
        if (val.equals("true")) {
          debug = true;
          enableJavaSystemOutAndErr();
        } else if (val.equals("false")) {
          debug = false;
          disableJavaSystemOutAndErr();
        } else
          return -2;
        break;
      default:
        return -1;
    }

    return 0;
  }

  // TODO: Separate the values out of the logic part
  List<String> getPossibleValues(String var) {
    List<String> vals = new ArrayList<>();
    switch (var.toLowerCase()) {
      case debugEnvVar:
        vals.add("true");
        vals.add("false");
        return vals;
      default:
        return null;
    }
  }

  void printAll(Writer writer) throws IOException {
    writer.write(debugEnvVar);
    writer.write('=');
    writer.write(debug.toString());
    writer.write('\n');
  }

  private void disableJavaSystemOutAndErr() {
    PrintStream ps = new PrintStream(new NullOutputStream());
    System.setOut(ps);
    System.setErr(ps);
  }

  private void enableJavaSystemOutAndErr() {
    System.setOut(stdout);
    System.setErr(stderr);
  }

  void takeEffect() {
    if (debug) {
      enableJavaSystemOutAndErr();
    } else {
      // We control terminal directly; Forbid any Java System.out and System.err stuff so
      // any underlying output will not mess up the console
      disableJavaSystemOutAndErr();
    }
  }

  private class NullOutputStream extends OutputStream {
    public void close() {
    }

    public void flush() {
    }

    public void write(byte[] b) {
    }

    public void write(byte[] b, int off, int len) {
    }

    public void write(int b) {
    }
  }
}
