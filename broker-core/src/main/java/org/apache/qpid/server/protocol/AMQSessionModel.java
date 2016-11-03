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

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.qpid.protocol.AMQConstant;
import org.apache.qpid.server.logging.LogSubject;
import org.apache.qpid.server.model.Consumer;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.Session;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.util.Deletable;
import org.apache.qpid.transport.network.Ticker;

/**
 * Session model interface.
 * Extends {@link Comparable} to allow objects to be inserted into a {@link ConcurrentSkipListSet}
 * when monitoring the blocking and blocking of queues/sessions in {@link Queue}.
 */
public interface AMQSessionModel<T extends AMQSessionModel<T>> extends Comparable<AMQSessionModel>, Deletable<T>
{
    UUID getId();

    AMQPConnection<?> getAMQPConnection();

    void close();

    void close(AMQConstant cause, String message);

    LogSubject getLogSubject();

    void doTimeoutAction(String reason);

    void block(Queue<?> queue);

    void unblock(Queue<?> queue);

    void block();

    void unblock();

    boolean getBlocking();

    Object getConnectionReference();

    int getUnacknowledgedMessageCount();

    Long getTxnCount();
    Long getTxnStart();
    Long getTxnCommits();
    Long getTxnRejects();

    int getChannelId();

    int getConsumerCount();

    Collection<Consumer<?>> getConsumers();

    void addConsumerListener(ConsumerListener listener);

    void removeConsumerListener(ConsumerListener listener);

    void setModelObject(Session<?> session);

    Session<?> getModelObject();

    /**
     * Return the time the current transaction started.
     *
     * @return the time this transaction started or 0 if not in a transaction
     */
    long getTransactionStartTime();

    /**
     * Return the time of the last activity on the current transaction.
     *
     * @return the time of the last activity or 0 if not in a transaction
     */
    long getTransactionUpdateTime();

    void transportStateChanged();

    boolean processPending();

    void addTicker(Ticker ticker);
    void removeTicker(Ticker ticker);

    void ensureConsumersNoticedStateChange();
}
