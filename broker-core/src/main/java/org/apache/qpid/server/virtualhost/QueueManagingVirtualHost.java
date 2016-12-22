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

import static org.apache.qpid.server.model.Initialization.materialize;

import java.security.AccessControlContext;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.ManageableMessage;
import org.apache.qpid.server.model.ManagedAttribute;
import org.apache.qpid.server.model.ManagedContextDefault;
import org.apache.qpid.server.model.ManagedOperation;
import org.apache.qpid.server.model.ManagedStatistic;
import org.apache.qpid.server.model.Param;
import org.apache.qpid.server.model.Queue;
import org.apache.qpid.server.model.StatisticType;
import org.apache.qpid.server.model.StatisticUnit;
import org.apache.qpid.server.model.VirtualHost;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.security.auth.SocketConnectionMetaData;
import org.apache.qpid.server.stats.StatisticsGatherer;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.EventListener;
import org.apache.qpid.server.store.preferences.UserPreferencesCreator;

public interface QueueManagingVirtualHost<X extends QueueManagingVirtualHost<X>> extends VirtualHost<X>,
                                                                                         EventListener,
                                                                                         StatisticsGatherer,
                                                                                         UserPreferencesCreator,
                                                                                         EventLoggerProvider
{
    String HOUSEKEEPING_CHECK_PERIOD            = "housekeepingCheckPeriod";
    String STORE_TRANSACTION_IDLE_TIMEOUT_CLOSE = "storeTransactionIdleTimeoutClose";
    String STORE_TRANSACTION_IDLE_TIMEOUT_WARN  = "storeTransactionIdleTimeoutWarn";
    String STORE_TRANSACTION_OPEN_TIMEOUT_CLOSE = "storeTransactionOpenTimeoutClose";
    String STORE_TRANSACTION_OPEN_TIMEOUT_WARN  = "storeTransactionOpenTimeoutWarn";
    String HOUSE_KEEPING_THREAD_COUNT           = "houseKeepingThreadCount";
    String ENABLED_CONNECTION_VALIDATORS        = "enabledConnectionValidators";
    String DISABLED_CONNECTION_VALIDATORS       = "disabledConnectionValidators";
    String NUMBER_OF_SELECTORS                  = "numberOfSelectors";
    String CONNECTION_THREAD_POOL_SIZE          = "connectionThreadPoolSize";
    String GLOBAL_ADDRESS_DOMAINS               = "globalAddressDomains";
    String NODE_AUTO_CREATION_POLICIES = "nodeAutoCreationPolicies";


    String QUEUE_DEAD_LETTER_QUEUE_ENABLED      = "queue.deadLetterQueueEnabled";

    @ManagedContextDefault( name = "queue.deadLetterQueueEnabled")
    boolean DEFAULT_DEAD_LETTER_QUEUE_ENABLED = false;

    String DEFAULT_DLE_NAME_SUFFIX = "_DLE";
    String PROPERTY_DEAD_LETTER_EXCHANGE_SUFFIX = "qpid.broker_dead_letter_exchange_suffix";
    String PROPERTY_DEAD_LETTER_QUEUE_SUFFIX = "qpid.broker_dead_letter_queue_suffix";

    @ManagedContextDefault( name = "virtualhost.housekeepingCheckPeriod")
    long DEFAULT_HOUSEKEEPING_CHECK_PERIOD = 30000l;

    String CONNECTION_THREAD_POOL_KEEP_ALIVE_TIMEOUT = "connectionThreadPoolKeepAliveTimeout";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = QueueManagingVirtualHost.CONNECTION_THREAD_POOL_KEEP_ALIVE_TIMEOUT)
    long DEFAULT_CONNECTION_THREAD_POOL_KEEP_ALIVE_TIMEOUT = 60; // Minutes


    @ManagedContextDefault( name = "virtualhost.storeTransactionIdleTimeoutClose")
    public static final long DEFAULT_STORE_TRANSACTION_IDLE_TIMEOUT_CLOSE = 0l;
    @ManagedContextDefault( name = "virtualhost.housekeepingThreadCount")
    int DEFAULT_HOUSEKEEPING_THREAD_COUNT = 4;

    @ManagedAttribute( defaultValue = "${virtualhost.storeTransactionIdleTimeoutClose}",
            description = "The maximum length of time, in milliseconds, that an open store transaction may "
                          + "remain idle. If a transaction exceeds this threshold, the resource that "
                          + "created the transaction will be closed automatically.")
    long getStoreTransactionIdleTimeoutClose();

    @ManagedContextDefault( name = "virtualhost.storeTransactionIdleTimeoutWarn")
    public static final long DEFAULT_STORE_TRANSACTION_IDLE_TIMEOUT_WARN = 180000l;

    @ManagedAttribute( defaultValue = "${virtualhost.storeTransactionIdleTimeoutWarn}",
            description = "The maximum length of time, in milliseconds, that an open store transaction may "
                          + "remain idle. If a transaction exceeds this threshold, warnings will be "
                          + "written to the logs.")
    long getStoreTransactionIdleTimeoutWarn();

    @ManagedContextDefault( name = "virtualhost.storeTransactionOpenTimeoutClose")
    public static final long DEFAULT_STORE_TRANSACTION_OPEN_TIMEOUT_CLOSE = 0l;

    @ManagedAttribute( defaultValue = "${virtualhost.storeTransactionOpenTimeoutClose}",
            description = "The maximum length of time, in milliseconds, that a store transaction may "
                          + "remain open. If a transaction exceeds this threshold, the resource that "
                          + "created the transaction will be closed automatically.")
    long getStoreTransactionOpenTimeoutClose();

    @ManagedContextDefault( name = "virtualhost.storeTransactionOpenTimeoutWarn")
    public static final long DEFAULT_STORE_TRANSACTION_OPEN_TIMEOUT_WARN = 300000l;

    @ManagedAttribute( defaultValue = "${virtualhost.storeTransactionOpenTimeoutWarn}",
            description = "The maximum length of time, in milliseconds, that a store transaction may "
                          + "remain open. If a transaction exceeds this threshold, warnings will be "
                          + "written to the logs.")
    long getStoreTransactionOpenTimeoutWarn();


    @ManagedAttribute( defaultValue = "${queue.deadLetterQueueEnabled}", initialization = materialize)
    boolean isQueue_deadLetterQueueEnabled();

    @ManagedAttribute( defaultValue = "${virtualhost.housekeepingCheckPeriod}")
    long getHousekeepingCheckPeriod();

    String VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE = "virtualhost.connectionThreadPool.size";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE)
    long DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE = Math.max(Runtime.getRuntime().availableProcessors() * 2, 64);

    @ManagedAttribute( defaultValue = "${" + QueueManagingVirtualHost.VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE + "}")
    int getConnectionThreadPoolSize();

    String VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS = "virtualhost.connectionThreadPool.numberOfSelectors";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS)
    long DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS = Math.max(DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE/8, 1);

    @ManagedAttribute( defaultValue = "${" + QueueManagingVirtualHost.VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS + "}")
    int getNumberOfSelectors();



    @ManagedAttribute( defaultValue = "${virtualhost.housekeepingThreadCount}")
    int getHousekeepingThreadCount();

    @ManagedAttribute( defaultValue = "[]",
            description = "a list of policies used for auto-creating nodes (such as Queues or Exchanges) when an "
                          + "address is published to or subscribed from and no node matching the address currently "
                          + "exists. Each policy describes a pattern to match against the address, the circumstances "
                          + "when auto-creation should occur (on publish, on consume, or both), the type of node to be "
                          + "created, and the properties of the node.")
    List<NodeAutoCreationPolicy> getNodeAutoCreationPolicies();

    @ManagedContextDefault( name = "virtualhost.enabledConnectionValidators")
    String DEFAULT_ENABLED_VALIDATORS = "[]";

    @ManagedAttribute( defaultValue = "${virtualhost.enabledConnectionValidators}")
    List<String> getEnabledConnectionValidators();

    @ManagedContextDefault( name = "virtualhost.disabledConnectionValidators")
    String DEFAULT_DISABLED_VALIDATORS = "[]";

    @ManagedAttribute( defaultValue = "${virtualhost.disabledConnectionValidators}")
    List<String> getDisabledConnectionValidators();

    @ManagedAttribute( defaultValue = "[]")
    List<String> getGlobalAddressDomains();


    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Queues")
    long getQueueCount();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Exchanges")
    long getExchangeCount();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Connections")
    long getConnectionCount();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Inbound")
    long getBytesIn();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Outbound")
    long getBytesOut();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Inbound")
    long getMessagesIn();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Outbound")
    long getMessagesOut();


    @Override
    @ManagedOperation(nonModifying = true, changesConfiguredObjectState = false)
    Collection<? extends Connection<?>> getConnections();

    @ManagedOperation(nonModifying = true, changesConfiguredObjectState = false)
    Connection<?> getConnection(@Param(name="name", mandatory = true) String name);

    @ManagedOperation(secure = true,
            description = "Publishes a message to a specified address. "
                          + "Returns the number of queues onto which it has been placed, "
                          + " or zero, if the address routes to no queues.",
            changesConfiguredObjectState = false)
    int publishMessage(@Param(name = "message", mandatory = true)ManageableMessage message);

    @ManagedOperation(nonModifying = true,
            description = "Extract configuration",
            paramRequiringSecure = "includeSecureAttributes",
            changesConfiguredObjectState = false)
    Map<String,Object> extractConfig(@Param(name="includeSecureAttributes",
            description = "include attributes that may contain passwords or other "
                          + "confidential information",
            defaultValue = "false") boolean includeSecureAttributes);
    @ManagedOperation(nonModifying = true,
            description = "Extract message store content",
            secure = true,
            changesConfiguredObjectState = false)
    Content exportMessageStore();

    @ManagedOperation(description = "Import message store content",
            secure = true,
            changesConfiguredObjectState = false)
    void importMessageStore(@Param(name="source", description = "Extract file", mandatory = true)String source);

    @ManagedOperation(description = "Resets statistics on this object and all child objects", changesConfiguredObjectState = false, nonModifying = true)
    void resetStatistics();

    @ManagedOperation(nonModifying = true,
            description = "Returns metadata concerning the current connection",
            changesConfiguredObjectState = false)
    SocketConnectionMetaData getConnectionMetaData();

    Broker<?> getBroker();

    DurableConfigurationStore getDurableConfigurationStore();

    void executeTransaction(TransactionalOperation op);

    void executeTask(String name, Runnable task, AccessControlContext context);

    void scheduleHouseKeepingTask(long period, HouseKeepingTask task);

    ScheduledFuture<?> scheduleTask(long delay, Runnable timeoutTask);

    Queue<?> getAttainedQueue(UUID id);

    Queue<?> getAttainedQueue(String name);


    String getLocalAddress(String routingAddress);

    <T extends ConfiguredObject<?>> T getAttainedChildFromAddress(Class<T> childClass,
                                                                  String address);

    void setFirstOpening(boolean firstOpening);

    long getTargetSize();
    void setTargetSize(long targetSize);
    long getTotalQueueDepthBytes();

    MessageDestination getSystemDestination(String name);

    interface Transaction
    {
        void dequeue(QueueEntry entry);

        void copy(QueueEntry entry, Queue<?> queue);

        void move(QueueEntry entry, Queue<?> queue);

    }

    interface TransactionalOperation
    {
        void withinTransaction(Transaction txn);

        List<Long> getModifiedMessageIds();
    }
}
