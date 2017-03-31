/*
 *
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
 *
 */
package org.apache.qpid.server.protocol.v1_0;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.protocol.v1_0.type.BaseSource;
import org.apache.qpid.server.protocol.v1_0.type.BaseTarget;
import org.apache.qpid.server.protocol.v1_0.type.messaging.TerminusDurability;
import org.apache.qpid.server.protocol.v1_0.type.transport.Attach;

public interface Link_1_0<S extends BaseSource, T extends BaseTarget> extends LinkDefinition<S, T>
{
    ListenableFuture<? extends LinkEndpoint<S, T>> attach(Session_1_0 session, final Attach attach);

    void linkClosed();

    void discardEndpoint();

    void setSource(S source);

    void setTarget(T target);

    void setTermini(S source, T target);

    TerminusDurability getHighestSupportedTerminusDurability();
}
