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
package org.apache.qpid.server.model;

import java.util.Collection;
import java.util.Date;

@ManagedObject( creatable = false, amqpName = "org.apache.qpid.Connection")
public interface Connection<X extends Connection<X>> extends ConfiguredObject<X>
{

    // Attributes

    String STATE = "state";

    String CLIENT_ID = "clientId";
    String CLIENT_VERSION = "clientVersion";
    String INCOMING = "incoming";
    String LOCAL_ADDRESS = "localAddress";
    String PRINCIPAL = "principal";
    String PROPERTIES = "properties";
    String REMOTE_ADDRESS = "remoteAddress";
    String REMOTE_PROCESS_NAME = "remoteProcessName";
    String REMOTE_PROCESS_PID = "remoteProcessPid";
    String SESSION_COUNT_LIMIT = "sessionCountLimit";
    String TRANSPORT = "transport";
    String PORT = "port";


    String MAX_UNCOMMITTED_IN_MEMORY_SIZE = "connection.maxUncommittedInMemorySize";

    @ManagedContextDefault(name = MAX_UNCOMMITTED_IN_MEMORY_SIZE,
            description = "Defines the maximum limit of total messages sizes (in bytes) from uncommitted transactions"
                          + " which connection can hold in memory. If limit is breached, all messages from"
                          + " connection in-flight transactions are flowed to disk including those arriving"
                          + " after breaching the limit.")
    long DEFAULT_MAX_UNCOMMITTED_IN_MEMORY_SIZE = 10l * 1024l * 1024l;


    String CLOSE_RESPONSE_TIMEOUT = "connection.closeResponseTimeout";
    @ManagedContextDefault(name = CLOSE_RESPONSE_TIMEOUT)
    long DEFAULT_CLOSE_RESPONSE_TIMEOUT = 2000L;

    String MAX_MESSAGE_SIZE = "qpid.max_message_size";
    @ManagedContextDefault(name = MAX_MESSAGE_SIZE)
    int DEFAULT_MAX_MESSAGE_SIZE = 100 * 1024 * 1024;

    @DerivedAttribute
    String getClientId();

    @DerivedAttribute
    String getClientVersion();

    @DerivedAttribute
    String getClientProduct();

    @DerivedAttribute
    boolean isIncoming();

    @DerivedAttribute
    String getLocalAddress();

    @DerivedAttribute
    String getPrincipal();

    @DerivedAttribute
    String getRemoteAddress();

    @DerivedAttribute
    String getRemoteProcessName();

    @DerivedAttribute
    String getRemoteProcessPid();

    @DerivedAttribute(description = "The actual negotiated value of session count limit")
    int getSessionCountLimit();

    @DerivedAttribute
    Transport getTransport();

    @DerivedAttribute
    String getTransportInfo();

    @DerivedAttribute
    Protocol getProtocol();

    @DerivedAttribute
    NamedAddressSpace getAddressSpace();

    @DerivedAttribute
    Port<?> getPort();

    @DerivedAttribute(description = "The maximum size in bytes that uncommitted transactions associated with this connection"
                                    + " may grow before the messages contained within the transactions will be flowed to disk. "
                                    + " Disabled if negative.")
    long getMaxUncommittedInMemorySize();

    // currently this reports inbound message content size without header.
    // See also QPID-7689: https://issues.apache.org/jira/browse/QPID-7689?focusedCommentId=16022923#comment-16022923
    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Inbound",
                      description = "Total size of all messages received by this connection.")
    long getBytesIn();

    // currently this reports outbound message content size without header.
    // See also QPID-7689: https://issues.apache.org/jira/browse/QPID-7689?focusedCommentId=16022923#comment-16022923
    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Outbound",
                      description = "Total size of all messages delivered by this connection.")
    long getBytesOut();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Inbound",
                      description = "Total number of messages delivered by this connection.")
    long getMessagesIn();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Outbound",
                      description = "Total number of messages received by this connection.")
    long getMessagesOut();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.ABSOLUTE_TIME, label = "Last I/O time",
                      description = "Time of last I/O operation performed by this connection.")
    Date getLastIoTime();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.ABSOLUTE_TIME, label = "Last Inbound Message",
            description = "Time of last message received by the broker on this connection. "
                          + "If no message has been received the connection creation time will be used.")
    Date getLastInboundMessageTime();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.ABSOLUTE_TIME, label = "Last Outbound Message",
            description = "Time of last message sent by the broker on this connection. "
                          + "If no message has been snt the connection creation time will be used.")
    Date getLastOutboundMessageTime();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.ABSOLUTE_TIME, label = "Last Message",
            description = "Time of last message sent or received by the broker on this connection. "
                          + "If no message has been sent or received the connection creation time will be used.")
    Date getLastMessageTime();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Sessions",
                      description = "Current number of sessions belonging to this connection.")
    int getSessionCount();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.COUNT,
            label = "Transactions", description = "Total number of transactions started.")
    long getLocalTransactionBegins();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.COUNT,
            label = "Rolled-back Transactions", description = "Total number of rolled-back transactions.")
    long getLocalTransactionRollbacks();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT,
            label = "Open Transactions", description = "Current number of open transactions.")
    long getLocalTransactionOpen();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.ABSOLUTE_TIME,
            label= "Oldest transaction start time",
            description = "The start time of the oldest transaction or null if no transaction is in progress.")
    Date getOldestTransactionStartTime();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Transacted Inbound",
            description = "Total number of messages delivered by this connection within a transaction.")
    long getTransactedMessagesIn();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Transacted Outbound",
            description = "Total number of messages received by this connection within a transaction.")
    long getTransactedMessagesOut();

    //children
    Collection<Session> getSessions();

}
