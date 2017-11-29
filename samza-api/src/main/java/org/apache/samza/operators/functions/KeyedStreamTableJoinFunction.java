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

package org.apache.samza.operators.functions;

import org.apache.samza.operators.KV;


/**
 * Keyed stream-table join function, see {@link StreamTableJoinFunction} for more details.
 *
 * @param <K> type of the join key
 * @param <MV> type of the input message value
 * @param <RV> type of the table record value
 * @param <JM> type of join results
 */
public interface KeyedStreamTableJoinFunction<K, MV, RV, JM>
    extends StreamTableJoinFunction<K, KV<K, MV>, KV<K, RV>, JM> {

  default K getMessageKey(KV<K, MV> message) {
    return message.getKey();
  }

}
