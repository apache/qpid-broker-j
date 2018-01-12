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

package org.apache.qpid.server.protocol.v1_0;

import org.apache.qpid.server.bytebuffer.QpidByteBuffer;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.model.DerivedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedObject;
import org.apache.qpid.server.protocol.v1_0.codec.SectionDecoderRegistry;
import org.apache.qpid.server.protocol.v1_0.type.FrameBody;
import org.apache.qpid.server.protocol.v1_0.type.Symbol;
import org.apache.qpid.server.protocol.v1_0.type.codec.AMQPDescribedTypeRegistry;
import org.apache.qpid.server.protocol.v1_0.type.transport.End;
import org.apache.qpid.server.protocol.v1_0.type.transport.Error;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.ProtocolEngine;
import org.apache.qpid.server.txn.ServerTransaction;

@ManagedObject(category = false, creatable = false, type="AMQP_1_0")
public interface AMQPConnection_1_0<C extends AMQPConnection_1_0<C>> extends AMQPConnection<C>,
                                                                             ProtocolEngine,
                                                                             ConnectionHandler,
                                                                             EventLoggerProvider
{

    String CONNECTION_SESSION_CREDIT_WINDOW_SIZE = "connection.sessionCreditWindowSize";
    @ManagedContextDefault(name = CONNECTION_SESSION_CREDIT_WINDOW_SIZE)
    int DEFAULT_CONNECTION_SESSION_CREDIT_WINDOW_SIZE = 8192;

    String SEND_SASL_FINAL_CHALLENGE_AS_CHALLENGE = "connection.sendSaslFinalResponseAsChallenge";
    @ManagedContextDefault(name = SEND_SASL_FINAL_CHALLENGE_AS_CHALLENGE)
    boolean DEFAULT_SEND_SASL_FINAL_CHALLENGE_AS_CHALLENGE = false;

    Symbol ANONYMOUS_RELAY = Symbol.valueOf("ANONYMOUS-RELAY");
    Symbol SHARED_SUBSCRIPTIONS = Symbol.valueOf("SHARED-SUBS");

    @DerivedAttribute(description = "The idle timeout (in milliseconds) for incoming traffic.")
    long getIncomingIdleTimeout();

    @DerivedAttribute(description = "The period (in milliseconds) with which the Broker will generate heartbeat"
                                    + " traffic if the wire would otherwise be idle.")
    long getOutgoingIdleTimeout();

    Object getReference();

    String getRemoteContainerId();

    SectionDecoderRegistry getSectionDecoderRegistry();

    AMQPDescribedTypeRegistry getDescribedTypeRegistry();

    int sendFrame(int channel, FrameBody body, QpidByteBuffer payload);

    void sendFrame(int channel, FrameBody body);

    void sendEnd(int sendChannel, End end, boolean b);

    void sessionEnded(Session_1_0 session_1_0);

    boolean isClosed();

    void close(Error error);

    IdentifiedTransaction createIdentifiedTransaction();
    ServerTransaction getTransaction(int txnId);
    void removeTransaction(int txnId);

    void receivedComplete();

    @DerivedAttribute(description = "If true send a final SASL challenge using a SaslChallenge performative, rather than SaslOutcome.")
    boolean getSendSaslFinalChallengeAsChallenge();
}
