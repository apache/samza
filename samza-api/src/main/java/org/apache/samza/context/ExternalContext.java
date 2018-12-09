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
package org.apache.samza.context;

/**
 * An {@link ExternalContext} can be used to pass components created and managed outside of Samza into a Samza
 * application. This will be made accessible through the {@link Context}.
 * <p>
 * This is passed to {@link org.apache.samza.runtime.ApplicationRunner#run(ExternalContext)} and propagated down to the
 * {@link Context} object provided to tasks.
 * <p>
 * {@link ExternalContext} can be used to inject objects that need to be created by other frameworks, such as Spring.
 * <p>
 * This is currently just a marker interface for the object passed into Samza.
 */
public interface ExternalContext {
}
