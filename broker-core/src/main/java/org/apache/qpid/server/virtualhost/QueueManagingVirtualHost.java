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

import java.security.AccessControlContext;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledFuture;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.message.MessageDestination;
import org.apache.qpid.server.model.Broker;
import org.apache.qpid.server.model.ConfiguredObject;
import org.apache.qpid.server.model.Connection;
import org.apache.qpid.server.model.Content;
import org.apache.qpid.server.model.DerivedAttribute;
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
                                                                                         EventLoggerProvider,
                                                                                         CacheProvider
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
    String STATISTICS_REPORTING_PERIOD = "statisticsReportingPeriod";


    @ManagedContextDefault( name = "virtualhost.housekeepingCheckPeriod")
    long DEFAULT_HOUSEKEEPING_CHECK_PERIOD = 30000L;

    String FLOW_TO_DISK_CHECK_PERIOD = "virtualhost.flowToDiskCheckPeriod";
    @ManagedContextDefault(name = FLOW_TO_DISK_CHECK_PERIOD)
    long DEFAULT_FLOW_TO_DISK_CHECK_PERIOD = 30000L;

    String CONNECTION_THREAD_POOL_KEEP_ALIVE_TIMEOUT = "connectionThreadPoolKeepAliveTimeout";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = QueueManagingVirtualHost.CONNECTION_THREAD_POOL_KEEP_ALIVE_TIMEOUT)
    long DEFAULT_CONNECTION_THREAD_POOL_KEEP_ALIVE_TIMEOUT = 60; // Minutes


    @ManagedContextDefault( name = "virtualhost.storeTransactionIdleTimeoutClose")
    public static final long DEFAULT_STORE_TRANSACTION_IDLE_TIMEOUT_CLOSE = 0l;
    @ManagedContextDefault( name = "virtualhost.housekeepingThreadCount")
    int DEFAULT_HOUSEKEEPING_THREAD_COUNT = 4;

    String VIRTUALHOST_STATISTICS_REPORING_PERIOD = "virtualhost.statisticsReportingPeriod";
    @ManagedContextDefault(name = VIRTUALHOST_STATISTICS_REPORING_PERIOD)
    int DEFAULT_STATISTICS_REPORTING_PERIOD = 0;

    String DISCARD_GLOBAL_SHARED_SUBSCRIPTION_LINKS_ON_DETACH = "qpid.jms.discardGlobalSharedSubscriptionLinksOnDetach";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = DISCARD_GLOBAL_SHARED_SUBSCRIPTION_LINKS_ON_DETACH,
                           description = "If true AMQP 1.0 links of global shared subscriptions are discarded when the"
                                         + " link detaches. This is to avoid leaking links with the Qpid JMS client.")
    boolean DEFAULT_DISCARD_GLOBAL_SHARED_SUBSCRIPTION_LINKS_ON_DETACH = true;

    String CONNECTION_FREQUENCY_PERIOD = "qpid.virtualhost.connectionFrequencyPeriodInMillis";
    @ManagedContextDefault(name = CONNECTION_FREQUENCY_PERIOD, description = "Interval (in milliseconds) to evaluate connection frequency")
    @SuppressWarnings("unused")
    long DEFAULT_CONNECTION_FREQUENCY_PERIOD = 60 * 1000;

    @ManagedAttribute( defaultValue = "${" + VIRTUALHOST_STATISTICS_REPORING_PERIOD + "}", description = "Period (in seconds) of the statistic report.")
    int getStatisticsReportingPeriod();

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


    @ManagedAttribute( defaultValue = "${virtualhost.housekeepingCheckPeriod}")
    long getHousekeepingCheckPeriod();

    @DerivedAttribute( description = "Time (in milliseconds) between checks whether existing messages "
                                     + "need to be flowed to disk in order to free memory." )
    long getFlowToDiskCheckPeriod();

    @DerivedAttribute( description = "Indicates whether global shared durable subscriptions are disabled")
    boolean isDiscardGlobalSharedSubscriptionLinksOnDetach();

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

    String NAMED_CACHE_MAXIMUM_SIZE = "virtualhost.namedCache.maximumSize";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = NAMED_CACHE_MAXIMUM_SIZE, description = "Maximum number of entries within the named cached")
    int DEFAULT_NAMED_CACHE_SIZE = 100;
    String NAMED_CACHE_MAXIMUM_SIZE_FORMAT = "virtualhost.namedCache.%s.maximumSize";

    String NAMED_CACHE_EXPIRATION = "virtualhost.namedCache.expiration";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = NAMED_CACHE_EXPIRATION, description = "Expiration time (in millis) applied to cached values within the named cache")
    long DEFAULT_NAMED_CACHE_EXPIRATION = 300 * 1000;
    String NAMED_CACHE_EXPIRATION_FORMAT = "virtualhost.namedCache.%s.expiration";

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

    @Override
    @ManagedAttribute( defaultValue = "[]")
    List<String> getGlobalAddressDomains();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Queues",
                      description = "Current number of queues on this virtualhost.")
    long getQueueCount();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Exchanges",
                      description = "Current number of exchanges on this virtualhost.")
    long getExchangeCount();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.COUNT, label = "Connections",
                      description = "Current number of messaging connections made to this virtualhost.")
    long getConnectionCount();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.COUNT, label = "Total Connections",
            description = "Total number of messaging connections made to this virtualhost since broker startup")
    long getTotalConnectionCount();


    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Inbound",
                      description = "Total size of all messages received by this virtualhost.")
    long getBytesIn();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Outbound",
                      description = "Total size of all messages delivered by this virtualhost.")
    long getBytesOut();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Inbound",
                      description = "Total number of messages received by this virtualhost.")
    long getMessagesIn();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Outbound",
                      description = "Total number of messages delivered by this virtualhost.")
    long getMessagesOut();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES,
            label = "Transacted Inbound",
            description = "Total number of messages delivered by this virtualhost within a transaction.")
    long getTransactedMessagesIn();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES,
            label = "Transacted Outbound",
            description = "Total number of messages received by this virtualhost within a transaction.")
    long getTransactedMessagesOut();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "Queue Depth",
            description = "Current size of all messages enqueued by this virtualhost.")
    long getTotalDepthOfQueuesBytes();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.MESSAGES, label = "Queue Depth",
                      description = "Current number of messages enqueued by this virtualhost.")
    long getTotalDepthOfQueuesMessages();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME, units = StatisticUnit.BYTES, label = "In-Memory Message Bytes",
                      description="Current size of all messages cached in-memory.")
    long getInMemoryMessageSize();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Evacuated Message Bytes",
                      description = "Total Number of Bytes Evacuated from Memory Due to Flow to Disk.")
    long getBytesEvacuatedFromMemory();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
            units = StatisticUnit.BYTES,
            label = "Maximum recorded size of inbound messages",
            description = "Maximum size of message published into the Virtual Host since start-up.")
    long getInboundMessageSizeHighWatermark();

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

    @ManagedOperation(nonModifying = true,
            description = "Returns metadata concerning the current connection",
            changesConfiguredObjectState = false,
            skipAclCheck = true)
    SocketConnectionMetaData getConnectionMetaData();

    @SuppressWarnings("unused")
    @ManagedOperation(nonModifying = true, description = "Dumps link registry", changesConfiguredObjectState = false)
    Object dumpLinkRegistry();

    @SuppressWarnings("unused")
    @ManagedOperation(description = "Removes links with the given name and containerId pattern from the link registry.", changesConfiguredObjectState = false)
    void purgeLinkRegistry(@Param(name = "containerIdPattern", description = "Regular Expression to match the remote container id.", defaultValue = ".*") String containerIdPattern,
                           @Param(name = "role", description = "whether to remove only sending links (\"SENDER\"), receiving links (\"RECEIVER\") or both (\"BOTH\")", validValues = {"SENDER", "RECEIVER", "BOTH"}, defaultValue = "BOTH") String role,
                           @Param(name = "linkNamePattern", description = "Regular Expression to match the link names to be removed.", defaultValue = ".*") String linkNamePattern);

    Queue<?> getSubscriptionQueue(final String exchangeName,
                                  final Map<String, Object> attributes,
                                  final Map<String, Map<String, Object>> bindings);

    void removeSubscriptionQueue(final String queueName);

    Broker<?> getBroker();

    DurableConfigurationStore getDurableConfigurationStore();

    void executeTransaction(TransactionalOperation op);

    void executeTask(String name, Runnable task, AccessControlContext context);

    void scheduleHouseKeepingTask(long period, HouseKeepingTask task);

    ScheduledFuture<?> scheduleTask(long delay, Runnable timeoutTask);

    Queue<?> getAttainedQueue(UUID id);

    Queue<?> getAttainedQueue(String name);

    <T extends ConfiguredObject<?>> T getAttainedChildFromAddress(Class<T> childClass,
                                                                  String address);

    void setFirstOpening(boolean firstOpening);

    long getTargetSize();
    void setTargetSize(long targetSize);

    MessageDestination getSystemDestination(String name);

    ListenableFuture<Void> reallocateMessages();

    boolean isOverTargetSize();

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
