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
package org.apache.qpid.server.transport;

import java.net.SocketAddress;
import java.security.AccessControlContext;
import java.security.Principal;
import java.util.Collection;
import java.util.Iterator;

import javax.security.auth.Subject;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.port.AmqpPort;
import org.apache.qpid.server.session.AMQPSession;
import org.apache.qpid.server.txn.LocalTransaction;
import org.apache.qpid.server.txn.ServerTransaction;
import org.apache.qpid.server.util.Action;
import org.apache.qpid.server.util.Deletable;
import org.apache.qpid.server.virtualhost.ConnectionPrincipalStatistics;

public interface AMQPConnection<C extends AMQPConnection<C>>
        extends Connection<C>, Deletable<C>, EventLoggerProvider
{
    Broker<?> getBroker();

    long getConnectionId();

    AccessControlContext getAccessControlContextFromSubject(Subject subject);

    Subject getSubject();

    int getMessageCompressionThreshold();

    Principal getAuthorizedPrincipal();

    String getRemoteAddressString();

    String getAddressSpaceName();

    void notifyWork();

    String getRemoteContainerName();

    boolean isConnectionStopped();

    // currently this takes message content size without header.
    // See also QPID-7689: https://issues.apache.org/jira/browse/QPID-7689?focusedCommentId=16022923#comment-16022923
    void registerMessageReceived(long size);

    // currently this takes message content size without header.
    // See also QPID-7689: https://issues.apache.org/jira/browse/QPID-7689?focusedCommentId=16022923#comment-16022923
    void registerMessageDelivered(long size);

    void registerTransactedMessageReceived();

    void registerTransactedMessageDelivered();

    void closeSessionAsync(AMQPSession<?,?> session, CloseReason reason, String message);

    SocketAddress getRemoteSocketAddress();

    void block();

    void unblock();

    void updateLastMessageInboundTime();

    void updateLastMessageOutboundTime();

    void pushScheduler(NetworkConnectionScheduler networkConnectionScheduler);

    NetworkConnectionScheduler popScheduler();

    boolean hasSessionWithName(byte[] name);

    AggregateTicker getAggregateTicker();

    LocalTransaction createLocalTransaction();

    void incrementTransactionRollbackCounter();

    void decrementTransactionOpenCounter();

    void incrementTransactionOpenCounter();

    void incrementTransactionBeginCounter();

    Iterator<ServerTransaction> getOpenTransactions();

    void registerTransactionTickers(ServerTransaction serverTransaction,
                                    final Action<String> closeAction, final long notificationRepeatPeriod);

    void unregisterTransactionTickers(ServerTransaction serverTransaction);

    enum CloseReason
    {
        MANAGEMENT,
        TRANSACTION_TIMEOUT
    }

    void sendConnectionCloseAsync(CloseReason reason, String description);

    boolean isIOThread();
    ListenableFuture<Void> doOnIOThreadAsync(final Runnable task);

    void checkAuthorizedMessagePrincipal(String messageUserId);

    void stopConnection();

    /**
     * Returns the a view of session models.  Callers may not modify the returned view Required to return a copy.
     *
     * @return list of sessions
     */
    Collection<? extends AMQPSession<?,?>> getSessionModels();

    void notifyWork(AMQPSession<?,?> sessionModel);

    boolean isTransportBlockedForWriting();

    boolean isClosing();

    long getMaxMessageSize();

    @Override
    AmqpPort<?> getPort();

    void registered(ConnectionPrincipalStatistics connectionPrincipalStatistics);

    int getAuthenticatedPrincipalConnectionCount();

    int getAuthenticatedPrincipalConnectionFrequency();

}
