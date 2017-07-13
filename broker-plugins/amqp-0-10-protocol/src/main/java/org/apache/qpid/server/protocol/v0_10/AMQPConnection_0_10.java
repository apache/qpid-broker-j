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
 *
 */

package org.apache.qpid.server.protocol.v0_10;

import java.security.AccessControlContext;

import javax.security.auth.Subject;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.model.ContextProvider;
import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.ProtocolEngine;

@ManagedObject(category = false, creatable = false, type="AMQP_0_10")
public interface AMQPConnection_0_10<C extends AMQPConnection_0_10<C>> extends AMQPConnection<C>,
                                                                             ProtocolEngine,
                                                                             EventLoggerProvider
{
    // 0-10's current implementation (ServerConnection etc) means we have to break the encapsulation

    void initialiseHeartbeating(long writerIdle, long readerIdle);

    void setClientId(String clientId);

    void setClientProduct(String clientProduct);

    void setClientVersion(String clientVersion);

    void setRemoteProcessPid(String remoteProcessPid);

    void setSubject(Subject authorizedSubject);

    void setAddressSpace(NamedAddressSpace addressSpace);

    ContextProvider getContextProvider();

    AccessControlContext getAccessControllerContext();

    void performDeleteTasks();

    @DerivedAttribute(description = "The actual negotiated value of heartbeat delay.")
    int getHeartbeatDelay();

}
