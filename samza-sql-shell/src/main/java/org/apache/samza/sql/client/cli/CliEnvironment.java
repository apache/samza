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
    private String m_defaultPersistenceLocation;
    private String m_defaultPersistenceFileName = ".samzasqlshellrc";
    private static PrintStream m_stdout = System.out;
    private static PrintStream m_stderr = System.err;

    private ExecutionContext.MessageFormat m_messageFormat = ExecutionContext.MessageFormat.COMPACT;
    private static final String m_messageFormatEnvVar = "OUTPUT";

    private Boolean m_debug = false;
    private static final String m_debugEnvVar = "DEBUG";


    public CliEnvironment() {
        m_defaultPersistenceLocation = System.getProperty("user.home");
        if(m_defaultPersistenceLocation == null || m_defaultPersistenceLocation.isEmpty()) {
            m_defaultPersistenceLocation = System.getProperty("user.dir");
        }
        load();
        setupEnvironment();
    }

    private void load() {
        File file = new File(m_defaultPersistenceLocation, m_defaultPersistenceFileName);
        if (!file.exists())
            return;

        try {
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.startsWith("#"))
                    continue;
                String[] strs = line.split("=");
                if (strs.length != 2)
                    continue;
                setEnvironmentVariable(strs[0], strs[1]);
            }
            bufferedReader.close();
        } catch (FileNotFoundException e) {
            return;
        } catch (IOException e) {
            return;
        }
    }

    public ExecutionContext.MessageFormat getMessageFormat() {
        return m_messageFormat;
    }

    public void setMessageFormat(ExecutionContext.MessageFormat messageFormat) {
        m_messageFormat = messageFormat;
    }

    public boolean isDebug() {
        return m_debug;
    }

    public void setDebug(Boolean debug) {
        m_debug = debug;
    }

    public ExecutionContext generateExecutionContext() {
        ExecutionContext exeCtxt = new ExecutionContext();
        exeCtxt.setMessageFormat(m_messageFormat);
        return exeCtxt;
    }

    /**
     *
     * @param var
     * @param val
     * @return 0 : succeed
     *         -1: invalid var
     *         -2: invalid val
     */
    public int setEnvironmentVariable(String var, String val) {
        switch (var.toUpperCase()) {
            case m_messageFormatEnvVar:
                ExecutionContext.MessageFormat messageFormat = null;
                try {
                    messageFormat = ExecutionContext.MessageFormat.valueOf(val.toUpperCase());
                } catch(IllegalArgumentException e) {
                }
                if(messageFormat == null) {
                    return -2;
                }
                m_messageFormat = messageFormat;
                break;
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
        switch (var.toUpperCase()) {
            case m_messageFormatEnvVar:
                for(ExecutionContext.MessageFormat fm : ExecutionContext.MessageFormat.values()) {
                    vals.add(fm.toString());
                }
                return vals;
            case m_debugEnvVar:
                vals.add("true");
                vals.add("false");
                return vals;
            default:
                return null;
        }
    }

    public void printAll(Writer writer) throws IOException {
        writer.write(m_messageFormatEnvVar);
        writer.write('=');
        writer.write(m_messageFormat.name());
        writer.write('\n');

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

    private void setupEnvironment() {
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
