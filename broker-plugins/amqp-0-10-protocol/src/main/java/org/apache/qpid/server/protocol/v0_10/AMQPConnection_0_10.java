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

import javax.security.auth.Subject;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.ContextProvider;
import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.model.NamedAddressSpace;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.ProtocolEngine;

@ManagedObject(category = false, creatable = false, type="AMQP_0_10")
public interface AMQPConnection_0_10<C extends AMQPConnection_0_10<C>> extends AMQPConnection<C>,
                                                                             ProtocolEngine,
                                                                             EventLoggerProvider
{
    String CODEC_MAX_NESTED_OBJECTS = Connection.AMQP_0_X_CODEC_MAX_NESTED_OBJECTS;
    int DEFAULT_CODEC_MAX_NESTED_OBJECTS = Connection.DEFAULT_AMQP_0_X_CODEC_MAX_NESTED_OBJECTS;

    // 0-10's current implementation (ServerConnection etc) means we have to break the encapsulation

    String CONNECTION_MAX_UNASSEMBLED_SEGMENT_BYTES = "connection.maxUnassembledSegmentBytes";
    @ManagedContextDefault(name = CONNECTION_MAX_UNASSEMBLED_SEGMENT_BYTES,
            description = "Maximum aggregate bytes retained by a connection while reassembling fragmented " +
                    "segments. The effective limit is also constrained by qpid.max_message_size.")
    int DEFAULT_MAX_UNASSEMBLED_SEGMENT_BYTES = Connection.DEFAULT_MAX_MESSAGE_SIZE;

    String CONNECTION_MAX_UNASSEMBLED_SEGMENT_FRAMES = "connection.maxUnassembledSegmentFrames";
    @ManagedContextDefault(name = CONNECTION_MAX_UNASSEMBLED_SEGMENT_FRAMES,
            description = "Maximum aggregate frames retained by a connection while reassembling fragmented " +
                    "segments.")
    int DEFAULT_MAX_UNASSEMBLED_SEGMENT_FRAMES = 256 * 1024;

    void initialiseHeartbeating(long writerIdle, long readerIdle);

    void setClientId(String clientId);

    void setClientProduct(String clientProduct);

    void setClientVersion(String clientVersion);

    void setRemoteProcessPid(String remoteProcessPid);

    void setSubject(Subject authorizedSubject);

    void setAddressSpace(NamedAddressSpace addressSpace);

    ContextProvider getContextProvider();

    void performDeleteTasks();

    @DerivedAttribute(description = "The actual negotiated value of heartbeat delay.")
    int getHeartbeatDelay();

    int getMaxNestedObjects();

    int getMaxZeroWidthArrayElements();
}
