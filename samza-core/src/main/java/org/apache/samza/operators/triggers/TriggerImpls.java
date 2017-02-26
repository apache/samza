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

package org.apache.samza.operators.triggers;

import org.apache.samza.operators.data.MessageEnvelope;

/**
 * Factory methods for instantiating {@link TriggerImpl}s from individual {@link Trigger}s.
 */
public class TriggerImpls {

  public static <M extends MessageEnvelope> TriggerImpl<M> createTriggerImpl(Trigger<M> trigger, TriggerContext context, TriggerImpl.TriggerCallbackHandler handler) {

    if (trigger instanceof CountTrigger) {
      return new CountTriggerImpl<>((CountTrigger) trigger, context, handler);
    } else if (trigger instanceof RepeatingTrigger) {
      return new RepeatingTriggerImpl<>((RepeatingTrigger) trigger, context, handler);
    } else if (trigger instanceof AnyTrigger) {
      return new AnyTriggerImpl<>((AnyTrigger) trigger, context, handler);
    } else if (trigger instanceof TimeSinceLastMessageTrigger) {
      return new TimeSinceLastMessageTriggerImpl<>((TimeSinceLastMessageTrigger) trigger, context, handler);
    } else if (trigger instanceof TimeTrigger) {
      return new TimeTriggerImpl((TimeTrigger) trigger, context, handler);
    } else if (trigger instanceof TimeSinceFirstMessageTrigger) {
      return new TimeSinceFirstMessageTriggerImpl<>((TimeSinceFirstMessageTrigger) trigger, context, handler);
    }
    return null;
  }
}
