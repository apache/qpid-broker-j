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

import org.apache.qpid.framing.AMQDataBlock;
import org.apache.qpid.framing.MethodRegistry;
import org.apache.qpid.framing.ProtocolVersion;
import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ContextProvider;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.ProtocolEngine;

interface AMQPConnection_0_8<C extends AMQPConnection_0_8<C>> extends AMQPConnection<C>, ProtocolEngine, EventLoggerProvider
{
    int  DEFAULT_HEARTBEAT_TIMEOUT_FACTOR = 2;
    String PROPERTY_HEARTBEAT_TIMEOUT_FACTOR = "qpid.broker_heartbeat_timeout_factor";
    int HEARTBEAT_TIMEOUT_FACTOR = Integer.getInteger(PROPERTY_HEARTBEAT_TIMEOUT_FACTOR, DEFAULT_HEARTBEAT_TIMEOUT_FACTOR);

    Broker<?> getBroker();

    MethodRegistry getMethodRegistry();

    void writeFrame(AMQDataBlock frame);

    void sendConnectionClose(AMQConstant errorCode,
                             String message, int channelId);

    boolean isCloseWhenNoRoute();

    ContextProvider getContextProvider();

    boolean isClosing();

    void closeChannelOk(int channelId);

    int getBinaryDataLimit();

    long getMaxMessageSize();

    boolean ignoreAllButCloseOk();

    boolean channelAwaitingClosure(int channelId);

    ProtocolVersion getProtocolVersion();

    void closeChannel(AMQChannel amqChannel);

    boolean isSendQueueDeleteOkRegardless();

    void closeChannelAndWriteFrame(AMQChannel amqChannel, AMQConstant cause, String message);

    ProtocolOutputConverter getProtocolOutputConverter();

    Object getReference();

    ClientDeliveryMethod createDeliveryMethod(int channelId);

    void setDeferFlush(boolean batch);
}
