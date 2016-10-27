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

import static org.apache.qpid.server.model.Initialization.materialize;

import java.security.AccessControlContext;
import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.queue.QueueEntry;
import org.apache.qpid.server.store.DurableConfigurationStore;
import org.apache.qpid.server.store.MessageStore;
import org.apache.qpid.server.store.preferences.UserPreferencesCreator;
import org.apache.qpid.server.transport.AMQPConnection;
import org.apache.qpid.server.virtualhost.HouseKeepingTask;
import org.apache.qpid.server.virtualhost.NodeAutoCreationPolicy;

@ManagedObject( defaultType = "ProvidedStore", description = VirtualHost.CLASS_DESCRIPTION)
public interface VirtualHost<X extends VirtualHost<X>> extends ConfiguredObject<X>,
                                                               EventLoggerProvider, NamedAddressSpace,
                                                               UserPreferencesCreator
{
    String CLASS_DESCRIPTION = "<p>A virtualhost is a namespace in which messaging is performed. Virtualhosts are "
                               + "independent; the messaging goes on a within a virtualhost is independent of any "
                               + "messaging that goes on in another virtualhost. For instance, a queue named <i>foo</i> "
                               + "defined in one virtualhost is completely independent of a queue named <i>foo</i> in "
                               + "another virtualhost.</p>"
                               + "<p>A virtualhost is backed by storage which is used to store the messages.</p>";

    String QUEUE_DEAD_LETTER_QUEUE_ENABLED            = "queue.deadLetterQueueEnabled";

    String HOUSEKEEPING_CHECK_PERIOD            = "housekeepingCheckPeriod";
    String STORE_TRANSACTION_IDLE_TIMEOUT_CLOSE = "storeTransactionIdleTimeoutClose";
    String STORE_TRANSACTION_IDLE_TIMEOUT_WARN  = "storeTransactionIdleTimeoutWarn";
    String STORE_TRANSACTION_OPEN_TIMEOUT_CLOSE = "storeTransactionOpenTimeoutClose";
    String STORE_TRANSACTION_OPEN_TIMEOUT_WARN  = "storeTransactionOpenTimeoutWarn";
    String HOUSE_KEEPING_THREAD_COUNT           = "houseKeepingThreadCount";
    String MODEL_VERSION                        = "modelVersion";
    String ENABLED_CONNECTION_VALIDATORS        = "enabledConnectionValidators";
    String DISABLED_CONNECTION_VALIDATORS       = "disabledConnectionValidators";
    String GLOBAL_ADDRESS_DOMAINS               = "globalAddressDomains";
    String VIRTUALHOST_WORK_DIR_VAR             = "virtualhost.work_dir";
    String VIRTUALHOST_WORK_DIR_VAR_EXPRESSION  = "${qpid.work_dir}${file.separator}${ancestor:virtualhost:name}";
    String NUMBER_OF_SELECTORS                  = "numberOfSelectors";
    String CONNECTION_THREAD_POOL_SIZE          = "connectionThreadPoolSize";
    String CONNECTION_THREAD_POOL_KEEP_ALIVE_TIMEOUT = "connectionThreadPoolKeepAliveTimeout";
    String PREFERENCE_STORE_ATTRIBUTES          = "preferenceStoreAttributes";

    String NODE_AUTO_CREATION_POLICIES = "nodeAutoCreationPolicies";

    @ManagedContextDefault( name = VIRTUALHOST_WORK_DIR_VAR)
    public static final String VIRTUALHOST_WORK_DIR = VIRTUALHOST_WORK_DIR_VAR_EXPRESSION;
    @ManagedContextDefault( name = "queue.deadLetterQueueEnabled")
    public static final boolean DEFAULT_DEAD_LETTER_QUEUE_ENABLED = false;
    String DEFAULT_DLE_NAME_SUFFIX = "_DLE";

    @ManagedAttribute( defaultValue = "${queue.deadLetterQueueEnabled}", initialization = materialize)
    boolean isQueue_deadLetterQueueEnabled();

    @ManagedContextDefault( name = "virtualhost.housekeepingCheckPeriod")
    public static final long DEFAULT_HOUSEKEEPING_CHECK_PERIOD = 30000l;

    @SuppressWarnings("unused")
    @ManagedContextDefault(name = CONNECTION_THREAD_POOL_KEEP_ALIVE_TIMEOUT)
    long DEFAULT_CONNECTION_THREAD_POOL_KEEP_ALIVE_TIMEOUT = 60; // Minutes

    @ManagedAttribute( defaultValue = "${virtualhost.housekeepingCheckPeriod}")
    long getHousekeepingCheckPeriod();

    @ManagedContextDefault( name = "virtualhost.storeTransactionIdleTimeoutClose")
    public static final long DEFAULT_STORE_TRANSACTION_IDLE_TIMEOUT_CLOSE = 0l;

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

    @ManagedContextDefault( name = "virtualhost.housekeepingThreadCount")
    public static final int DEFAULT_HOUSEKEEPING_THREAD_COUNT = 4;

    @ManagedAttribute( defaultValue = "${virtualhost.housekeepingThreadCount}")
    int getHousekeepingThreadCount();

    @ManagedAttribute( defaultValue = "[]",
            description = "a list of policies used for auto-creating nodes (such as Queues or Exchanges) when an "
                          + "address is published to or subscribed from and no node matching the address currently "
                          + "exists. Each policy describes a pattern to match against the address, the circumstances "
                          + "when auto-creation should occur (on publish, on consume, or both), the type of node to be "
                          + "created, and the properties of the node.")
    List<NodeAutoCreationPolicy> getNodeAutoCreationPolicies();

    String VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE = "virtualhost.connectionThreadPool.size";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE)
    long DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE = Math.max(Runtime.getRuntime().availableProcessors() * 2, 64);

    @ManagedAttribute( defaultValue = "${" + VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE + "}")
    int getConnectionThreadPoolSize();

    String VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS = "virtualhost.connectionThreadPool.numberOfSelectors";
    @SuppressWarnings("unused")
    @ManagedContextDefault( name = VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS)
    long DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS = Math.max(DEFAULT_VIRTUALHOST_CONNECTION_THREAD_POOL_SIZE/8, 1);

    @ManagedAttribute( defaultValue = "${" + VIRTUALHOST_CONNECTION_THREAD_POOL_NUMBER_OF_SELECTORS + "}")
    int getNumberOfSelectors();

    @DerivedAttribute( persist = true )
    String getModelVersion();

    @ManagedContextDefault( name = "virtualhost.enabledConnectionValidators")
    String DEFAULT_ENABLED_VALIDATORS = "[]";

    void executeTask(String name, Runnable task, AccessControlContext context);

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

    Broker<?> getBroker();

    // LQ TODO: I think this is not being processed correctly because it is not annotated on the base. At least is does not show up in the generated overrides
    @Override
    @ManagedOperation(nonModifying = true, changesConfiguredObjectState = false)
    Collection<? extends Connection<?>> getConnections();

    @ManagedOperation(nonModifying = true, changesConfiguredObjectState = false)
    Connection<?> getConnection(@Param(name="name") String name);

    @ManagedOperation(secure = true,
                      description = "Publishes a message to a specified address. "
                                    + "Returns the number of queues onto which it has been placed, "
                                    + " or zero, if the address routes to no queues.",
            changesConfiguredObjectState = false)
    int publishMessage(@Param(name = "message")ManageableMessage message);

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
    void importMessageStore(@Param(name="source", description = "Extract file")String source);


    void start();

    void stop();

    Principal getPrincipal();

    void registerConnection(AMQPConnection<?> connection);
    void deregisterConnection(AMQPConnection<?> connection);

    Queue<?> getAttainedQueue(UUID id);

    <T extends ConfiguredObject<?>> T getAttainedChildFromAddress(Class<T> childClass,
                                                                  String address);

    MessageDestination getDefaultDestination();

    DurableConfigurationStore getDurableConfigurationStore();

    void scheduleHouseKeepingTask(long period, HouseKeepingTask task);

    ScheduledFuture<?> scheduleTask(long delay, Runnable timeoutTask);

    String getLocalAddress(String routingAddress);

    void setFirstOpening(boolean firstOpening);

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

    void executeTransaction(TransactionalOperation op);

    MessageStore getMessageStore();

    String getType();

    void setTargetSize(long targetSize);
    long getTargetSize();

    long getTotalQueueDepthBytes();

}
