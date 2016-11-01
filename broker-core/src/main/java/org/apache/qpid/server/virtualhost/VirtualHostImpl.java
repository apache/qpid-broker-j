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
package org.apache.qpid.server.virtualhost;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.exchange.ExchangeImpl;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.message.MessageSource;
import org.apache.qpid.server.model.NoFactoryForTypeException;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.protocol.LinkRegistry;
import org.apache.qpid.server.queue.AMQQueue;
import org.apache.qpid.server.security.SecurityManager;
import org.apache.qpid.server.stats.StatisticsGatherer;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.transport.AbstractAMQPConnection;
import org.apache.qpid.server.txn.DtxRegistry;

public interface VirtualHostImpl< X extends VirtualHostImpl<X,Q,E>, Q extends AMQQueue<?>, E extends ExchangeImpl<?> >
        extends EventLoggerProvider,
                VirtualHost<X,Q,E>
{
    String DEFAULT_DLE_NAME_SUFFIX = "_DLE";

    String getName();

    Q getAttainedQueue(String name);
    Q getAttainedQueue(UUID id);

    MessageSource getAttainedMessageSource(String name);

    Collection<Q> getQueues();

    int removeQueue(Q queue);

    ListenableFuture<Integer> removeQueueAsync(Q queue);

    Q createQueue(Map<String, Object> arguments) throws QueueExistsException;

    E createExchange(Map<String,Object> attributes)
            throws ExchangeExistsException, ReservedExchangeNameException,
                   NoFactoryForTypeException;

    E getAttainedExchange(String name);

    MessageDestination getAttainedMessageDestination(String name);


    MessageDestination getDefaultDestination();

    Collection<E> getExchanges();

    DurableConfigurationStore getDurableConfigurationStore();

    MessageStore getMessageStore();

    SecurityManager getSecurityManager();

    void scheduleHouseKeepingTask(long period, HouseKeepingTask task);

    long getHouseKeepingTaskCount();

    public long getHouseKeepingCompletedTaskCount();

    int getHouseKeepingPoolSize();

    void setHouseKeepingPoolSize(int newSize);

    int getHouseKeepingActiveCount();

    DtxRegistry getDtxRegistry();

    LinkRegistry getLinkRegistry(String remoteContainerId);

    ScheduledFuture<?> scheduleTask(long delay, Runnable timeoutTask);

    boolean getDefaultDeadLetterQueueEnabled();

    EventLogger getEventLogger();

    boolean authoriseCreateConnection(AMQPConnection<?> connection);

    String getLocalAddress(String routingAddress);

}
