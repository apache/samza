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

import org.apache.samza.sql.client.interfaces.ExecutionContext;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CliEnvironment {
    private static PrintStream m_stdout = System.out;
    private static PrintStream m_stderr = System.err;


    private Boolean m_debug = false;
    private static final String m_debugEnvVar = "shell.debug";

    public boolean isDebug() {
        return m_debug;
    }

    public void setDebug(Boolean debug) {
        m_debug = debug;
    }

    /**
     *
     * @param var Environment variable
     * @param val Value of the environment variable
     * @return 0 : succeed
     *         -1: invalid var
     *         -2: invalid val
     */
    public int setEnvironmentVariable(String var, String val) {
        switch (var.toUpperCase()) {
            case m_debugEnvVar:
                val = val.toLowerCase();
                if(val.equals("true")) {
                    m_debug = true;
                    enableJavaSystemOutAndErr();
                }
                else if(val.equals("false")) {
                    m_debug = false;
                    disableJavaSystemOutAndErr();
                }
                else
                    return -2;
                break;
            default:
                return -1;
        }

        return 0;
    }

    public List<String> getPossibleValues(String var) {
        List<String> vals = new ArrayList<>();
        switch (var.toLowerCase()) {
            case m_debugEnvVar:
                vals.add("true");
                vals.add("false");
                return vals;
            default:
                return null;
        }
    }

    public void printAll(Writer writer) throws IOException {
        writer.write(m_debugEnvVar);
        writer.write('=');
        writer.write(m_debug.toString());
        writer.write('\n');
    }

    private void disableJavaSystemOutAndErr() {
        PrintStream ps = new PrintStream(new NullOutputStream());
        System.setOut(ps);
        System.setErr(ps);
    }

    private void enableJavaSystemOutAndErr() {
        System.setOut(m_stdout);
        System.setErr(m_stderr);
    }

    private class NullOutputStream extends OutputStream {
        public void close() {}
        public void flush() {}
        public void write(byte[] b) {}
        public void write(byte[] b, int off, int len) {}
        public void write(int b) {}
    }

    void takeEffect() {
        if(m_debug) {
            enableJavaSystemOutAndErr();
        }
        else {
            // We control terminal directly; Forbid any Java System.out and System.err stuff so
            // any underlying output will not mess up the console
            disableJavaSystemOutAndErr();
        }

    }
}
