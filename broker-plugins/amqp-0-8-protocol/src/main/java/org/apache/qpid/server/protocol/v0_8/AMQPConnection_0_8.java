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
package org.apache.qpid.server.protocol.v0_8;

import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.protocol.v0_8.transport.AMQDataBlock;
import org.apache.qpid.server.protocol.v0_8.transport.MethodRegistry;
import org.apache.qpid.server.protocol.ProtocolVersion;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.model.ContextProvider;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.ProtocolEngine;

@ManagedObject(category = false, creatable = false, type="AMQP_0_8")
public interface AMQPConnection_0_8<C extends AMQPConnection_0_8<C>> extends AMQPConnection<C>, ProtocolEngine, EventLoggerProvider
{

    String PROPERTY_HEARTBEAT_TIMEOUT_FACTOR = "qpid.broker_heartbeat_timeout_factor";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = PROPERTY_HEARTBEAT_TIMEOUT_FACTOR)
    int  DEFAULT_HEARTBEAT_TIMEOUT_FACTOR = 2;


    String HIGH_PREFETCH_LIMIT = "connection.high_prefetch_limit";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name= HIGH_PREFETCH_LIMIT)
    long DEFAULT_HIGH_PREFETCH_LIMIT = 100L;

    String BATCH_LIMIT = "connection.batch_limit";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name= BATCH_LIMIT)
    long DEFAULT_BATCH_LIMIT = 10L;

    String FORCE_MESSAGE_VALIDATION = "qpid.connection.forceValidation";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name= FORCE_MESSAGE_VALIDATION)
    boolean DEFAULT_FORCE_MESSAGE_VALIDATION = false;

    @DerivedAttribute(description = "The actual negotiated value of heartbeat delay.")
    int getHeartbeatDelay();

    MethodRegistry getMethodRegistry();

    void writeFrame(AMQDataBlock frame);

    void sendConnectionClose(int errorCode,
                             String message, int channelId);


    boolean isCloseWhenNoRoute();

    ContextProvider getContextProvider();

    void closeChannelOk(int channelId);

    int getBinaryDataLimit();

    boolean ignoreAllButCloseOk();

    boolean channelAwaitingClosure(int channelId);

    ProtocolVersion getProtocolVersion();

    void closeChannel(AMQChannel amqChannel);

    boolean isSendQueueDeleteOkRegardless();

    void closeChannelAndWriteFrame(AMQChannel amqChannel, int code, String message);

    ProtocolOutputConverter getProtocolOutputConverter();

    Object getReference();

    ClientDeliveryMethod createDeliveryMethod(int channelId);

    void setDeferFlush(boolean batch);
}
