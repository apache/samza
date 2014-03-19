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

package org.apache.samza.test.integration.join;

import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

public class Watcher implements StreamTask, WindowableTask, InitableTask {

  private boolean inError = false;
  private long lastEpochChange = System.currentTimeMillis();
  private long maxTimeBetweenEpochsMs;
  private int currentEpoch = 0;
  private String smtpHost;
  private String to;
  private String from;
  
  @Override
  public void init(Config config, TaskContext context) {
    this.maxTimeBetweenEpochsMs = config.getLong("max.time.between.epochs.ms");
    this.smtpHost = config.get("mail.smtp.host");
    this.to = config.get("mail.to");
    this.from = config.get("mail.from");
  }
  
  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    int epoch = Integer.parseInt((String) envelope.getMessage());
    if(epoch > currentEpoch) {
      this.currentEpoch = epoch;
      this.lastEpochChange = System.currentTimeMillis();
      this.inError = false;
    }
  }
  
  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
    boolean isLagging = System.currentTimeMillis() - lastEpochChange > maxTimeBetweenEpochsMs;
    if(!inError && isLagging) {
      this.inError = true;
      sendEmail(from, to, "Job failed to make progress!", String.format("No epoch change for %d minutes.", this.maxTimeBetweenEpochsMs / (60*1000)));
    }
  }
  
  private void sendEmail(String from, String to, String subject, String body) {
    Properties props = new Properties();
    props.put("mail.smtp.host", smtpHost);
    Session session = Session.getInstance(props, null);
    try {
        MimeMessage msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress(from));
        msg.addRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
        msg.setSubject(subject);
        msg.setSentDate(new Date());
        msg.setText(body);
        Transport.send(msg);
    } catch (MessagingException e) {
        throw new RuntimeException(e);
    }
  }
  
}
