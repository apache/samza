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

import java.util.ArrayList;
import org.apache.samza.sql.client.util.CliUtil;

import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.List;


public class CliHighlighter implements Highlighter {
    private static final List<String> keywords;
    static {
        keywords = CliCommandType.getAllCommands();
        keywords.add("FROM");
        keywords.add("WHERE");
    }
    
    public AttributedString highlight(LineReader reader, String buffer) {
        AttributedStringBuilder builder = new AttributedStringBuilder();
        List<String> tokens = splitWithSpace(buffer);

        for(String token : tokens) {
            if(isKeyword(token)) {
                builder.style(AttributedStyle.BOLD.foreground(AttributedStyle.YELLOW))
                        .append(token);
            } else {
                builder.style(AttributedStyle.DEFAULT)
                        .append(token);
            }
        }

        return builder.toAttributedString();
    }

    private boolean isKeyword(String token) {
        for(String keyword : keywords) {
            if(keyword.compareToIgnoreCase(token) == 0)
                return true;
        }
        return false;

    }

    private static List<String> splitWithSpace(String buffer) {
        List<String> list = new ArrayList<String>();
        if(CliUtil.isNullOrEmpty(buffer))
            return list;

        boolean prevIsSpace = Character.isSpaceChar(buffer.charAt(0));
        int prevPos = 0;
        for(int i = 1; i < buffer.length(); ++i) {
            char c = buffer.charAt(i);
            boolean isSpace = Character.isSpaceChar(c);
            if(isSpace != prevIsSpace) {
                list.add(buffer.substring(prevPos, i));
                prevPos = i;
                prevIsSpace = isSpace;
            }
        }
        list.add(buffer.substring(prevPos));
        return list;
    }
}
