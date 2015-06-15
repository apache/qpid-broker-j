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
package org.apache.qpid.server.protocol;

import javax.security.auth.Subject;

import org.apache.qpid.protocol.ProtocolEngine;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.virtualhost.VirtualHostImpl;
import org.apache.qpid.transport.network.AggregateTicker;
import org.apache.qpid.transport.network.NetworkConnection;

public interface ServerProtocolEngine extends ProtocolEngine
{
    /**
     * Gets the connection ID associated with this ProtocolEngine
     */
    long getConnectionId();

    Subject getSubject();

    boolean isTransportBlockedForWriting();

    void setTransportBlockedForWriting(boolean blocked);

    void setMessageAssignmentSuspended(boolean value);

    boolean isMessageAssignmentSuspended();

    void processPending();

    boolean hasWork();

    void clearWork();

    void notifyWork();

    void setWorkListener(Action<ServerProtocolEngine> listener);

    AggregateTicker getAggregateTicker();

}
