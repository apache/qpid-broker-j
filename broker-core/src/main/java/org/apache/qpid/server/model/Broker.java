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

import java.security.Principal;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.qpid.server.configuration.CommonProperties;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.security.auth.SocketConnectionMetaData;
import org.apache.qpid.server.stats.StatisticsGatherer;
import org.apache.qpid.server.store.preferences.UserPreferencesCreator;

@ManagedObject( defaultType = Broker.BROKER_TYPE, amqpName = "org.apache.qpid.Broker")
public interface Broker<X extends Broker<X>> extends ConfiguredObject<X>, EventLoggerProvider, StatisticsGatherer, Container<X>,
                                                     UserPreferencesCreator
{
    String BROKER_TYPE = "Broker";

    String BUILD_VERSION = "buildVersion";
    String OPERATING_SYSTEM = "operatingSystem";
    String PLATFORM = "platform";
    String PROCESS_PID = "processPid";
    String PRODUCT_VERSION = "productVersion";
    String STATISTICS_REPORTING_PERIOD = "statisticsReportingPeriod";
    String STORE_PATH = "storePath";
    String MODEL_VERSION = "modelVersion";
    String PREFERENCE_STORE_ATTRIBUTES   = "preferenceStoreAttributes";

    String CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT = "channel.flowControlEnforcementTimeout";
    String BROKER_FLOW_TO_DISK_THRESHOLD = "broker.flowToDiskThreshold";
    String BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD = "broker.failStartupWithErroredChild";
    String BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD_SCOPE = "broker.failStartupWithErroredChildScope";

    String BROKER_MSG_AUTH = "qpid.broker_msg_auth";

    String STORE_FILESYSTEM_MAX_USAGE_PERCENT = "store.filesystem.maxUsagePercent";
    String QPID_AMQP_PORT = "qpid.amqp_port";
    String QPID_HTTP_PORT = "qpid.http_port";
    String QPID_DOCUMENTATION_URL = "qpid.helpURL";
    String BROKER_STATISTICS_REPORING_PERIOD = "broker.statisticsReportingPeriod";

    String NETWORK_BUFFER_SIZE = "qpid.broker.networkBufferSize";
    // network buffer should at least hold a SSL/TLS frame which in jdk1.8 is 33305 bytes
    int MINIMUM_NETWORK_BUFFER_SIZE = 64 * 1024;
    @ManagedContextDefault(name = NETWORK_BUFFER_SIZE)
    int DEFAULT_NETWORK_BUFFER_SIZE = 256 * 1024;

    @ManagedContextDefault(name = "broker.name")
    String DEFAULT_BROKER_NAME = "Broker";

    @ManagedContextDefault(name = QPID_AMQP_PORT)
    String DEFAULT_AMQP_PORT_NUMBER = "5672";
    @ManagedContextDefault(name = QPID_HTTP_PORT)
    String DEFAULT_HTTP_PORT_NUMBER = "8080";

    @ManagedContextDefault(name = BROKER_FLOW_TO_DISK_THRESHOLD)
    long DEFAULT_FLOW_TO_DISK_THRESHOLD = (long)(0.75 * (double) BrokerImpl.getMaxDirectMemorySize());

    String COMPACT_MEMORY_THRESHOLD = "qpid.compact_memory_threshold";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = COMPACT_MEMORY_THRESHOLD)
    long DEFAULT_COMPACT_MEMORY_THRESHOLD = (long)(0.5 * (double) BrokerImpl.getMaxDirectMemorySize());

    String COMPACT_MEMORY_INTERVAL = "qpid.compact_memory_interval";
    @SuppressWarnings("unused")
    @ManagedContextDefault(name = COMPACT_MEMORY_INTERVAL)
    long DEFAULT_COMPACT_MEMORY_INTERVAL = 1000L;

    @ManagedContextDefault(name = CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT)
    long DEFAULT_CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT = 5000l;

    @ManagedContextDefault(name = STORE_FILESYSTEM_MAX_USAGE_PERCENT)
    int DEFAULT_FILESYSTEM_MAX_USAGE_PERCENT = 90;

    @ManagedContextDefault(name = BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD)
    boolean DEFAULT_BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD = false;

    @ManagedContextDefault(name = BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD_SCOPE)
    DescendantScope DEFAULT_BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD_SCOPE = DescendantScope.IMMEDIATE;

    @ManagedContextDefault(name = BROKER_MSG_AUTH)
    boolean DEFAULT_BROKER_MSG_AUTH = false;

    @ManagedContextDefault(name = CommonProperties.QPID_SECURITY_TLS_PROTOCOL_WHITE_LIST)
    String DEFAULT_SECURITY_TLS_PROTOCOL_WHITE_LIST =
            "[\"" + CommonProperties.QPID_SECURITY_TLS_PROTOCOL_WHITE_LIST_DEFAULT.replace("\\", "\\\\") + "\"]";

    @ManagedContextDefault(name = CommonProperties.QPID_SECURITY_TLS_PROTOCOL_BLACK_LIST)
    String DEFAULT_SECURITY_TLS_PROTOCOL_BLACK_LIST =
            "[\"" + CommonProperties.QPID_SECURITY_TLS_PROTOCOL_BLACK_LIST_DEFAULT.replace("\\", "\\\\") + "\"]";

    @ManagedContextDefault(name = CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_WHITE_LIST)
    String DEFAULT_SECURITY_TLS_CIPHER_SUITE_WHITE_LIST = "[]";

    @ManagedContextDefault(name = CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST)
    String DEFAULT_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST = "[]";

    @ManagedContextDefault(name = QPID_DOCUMENTATION_URL)
    String DEFAULT_DOCUMENTATION_URL = "http://qpid.apache.org/releases/qpid-broker-j-${qpid.version}/book/";

    @ManagedContextDefault(name = BROKER_STATISTICS_REPORING_PERIOD)
    int DEFAULT_STATISTICS_REPORTING_PERIOD = 0;

    String PROPERTY_DISABLED_FEATURES = "qpid.broker_disabled_features";

    @DerivedAttribute
    String getBuildVersion();

    @DerivedAttribute
    String getOperatingSystem();

    @DerivedAttribute
    String getPlatform();

    @DerivedAttribute
    String getProcessPid();

    @DerivedAttribute
    String getProductVersion();

    @DerivedAttribute
    int getNumberOfCores();

    @ManagedAttribute( defaultValue = "${" + BROKER_STATISTICS_REPORING_PERIOD + "}", description = "Period (in seconds) of the statistic report.")
    int getStatisticsReportingPeriod();

    @ManagedContextDefault( name = "broker.housekeepingThreadCount")
    public static final int DEFAULT_HOUSEKEEPING_THREAD_COUNT = 2;

    String QPID_BROKER_HOUSEKEEPING_CHECK_PERIOD = "qpid.broker.housekeepingCheckPeriod";
    @ManagedContextDefault(name = QPID_BROKER_HOUSEKEEPING_CHECK_PERIOD)
    long DEFAULT_BROKER_HOUSEKEEPING_CHECK_PERIOD = 30000L;

    @ManagedAttribute( defaultValue = "${broker.housekeepingThreadCount}")
    int getHousekeepingThreadCount();

    String BROKER_MESSAGE_COMPRESSION_ENABLED = "broker.messageCompressionEnabled";
    @ManagedContextDefault(name = BROKER_MESSAGE_COMPRESSION_ENABLED)
    boolean DEFAULT_MESSAGE_COMPRESSION_ENABLED = true;

    @ManagedAttribute( defaultValue = "${"+ BROKER_MESSAGE_COMPRESSION_ENABLED +"}")
    boolean isMessageCompressionEnabled();

    String MESSAGE_COMPRESSION_THRESHOLD_SIZE = "connection.messageCompressionThresholdSize";
    @ManagedContextDefault(name = MESSAGE_COMPRESSION_THRESHOLD_SIZE)
    int DEFAULT_MESSAGE_COMPRESSION_THRESHOLD_SIZE = 102400;

    String SEND_QUEUE_DELETE_OK_REGARDLESS_CLIENT_VER_REGEXP = "connection.sendQueueDeleteOkRegardlessClientVerRegexp";
    @ManagedContextDefault(name = SEND_QUEUE_DELETE_OK_REGARDLESS_CLIENT_VER_REGEXP)
    String DEFAULT_SEND_QUEUE_DELETE_OK_REGARDLESS_CLIENT_VER_REGEXP = "^0\\..*$";

    String BROKER_DIRECT_BYTE_BUFFER_POOL_SIZE = "broker.directByteBufferPoolSize";
    @ManagedContextDefault(name = BROKER_DIRECT_BYTE_BUFFER_POOL_SIZE)
    int DEFAULT_BROKER_DIRECT_BYTE_BUFFER_POOL_SIZE = 1024;

    String BROKER_DIRECT_BYTE_BUFFER_POOL_SPARSITY_REALLOCATION_FRACTION = "broker.directByteBufferPoolSparsityReallocationFraction";
    @ManagedContextDefault(name = BROKER_DIRECT_BYTE_BUFFER_POOL_SPARSITY_REALLOCATION_FRACTION)
    double DEFAULT_BROKER_DIRECT_BYTE_BUFFER_POOL_SPARSITY_REALLOCATION_FRACTION = 0.5;

    @ManagedAttribute(validValues = {"org.apache.qpid.server.model.BrokerImpl#getAvailableConfigurationEncrypters()"})
    String getConfidentialConfigurationEncryptionProvider();

    @DerivedAttribute( persist = true )
    String getModelVersion();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Inbound",
                      description = "Total size of all messages received by the Broker.")
    long getBytesIn();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Outbound",
                      description = "Total size of all messages delivered by the Broker.")
    long getBytesOut();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Inbound",
                      description = "Total number of messages received by the Broker.")
    long getMessagesIn();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Outbound",
                      description = "Total number of messages delivered by the Broker.")
    long getMessagesOut();


    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES,
            label = "Transacted Inbound",
            description = "Total number of messages delivered by the Broker within a transaction.")
    long getTransactedMessagesIn();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES,
            label = "Transacted Outbound",
            description = "Total number of messages received by the Broker within a transaction.")
    long getTransactedMessagesOut();

    @ManagedOperation(nonModifying = true,
            description = "Initiates an orderly shutdown of the Broker.",
            changesConfiguredObjectState = false)
    void initiateShutdown();

    @ManagedOperation(nonModifying = true,
            description = "Extract configuration",
            paramRequiringSecure = "includeSecureAttributes",
            changesConfiguredObjectState = false)
    Map<String,Object> extractConfig(@Param(name="includeSecureAttributes",
                                            description = "include attributes that may contain passwords or other"
                                                          + " confidential information",
                                            defaultValue = "false") boolean includeSecureAttributes);

    @DerivedAttribute(description = "Maximum heap memory size")
    long getMaximumHeapMemorySize();

    @DerivedAttribute(description = "Maximum direct memory size which can be consumed by broker")
    long getMaximumDirectMemorySize();

    @DerivedAttribute(description = "JVM arguments specified on startup")
    List<String> getJvmArguments();

    @DerivedAttribute(description = "URL to broker documentation")
    String getDocumentationUrl();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
                      units = StatisticUnit.COUNT,
                      label = "Live threads",
                      description = "Number of live threads")
    int getNumberOfLiveThreads();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
                      units = StatisticUnit.BYTES,
                      label = "Used Heap Memory Size",
                      description = "Size of used heap memory")
    long getUsedHeapMemorySize();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
                      units = StatisticUnit.BYTES,
                      label = "Used Direct Memory Size",
                      description = "Size of used direct memory")
    long getUsedDirectMemorySize();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
                      units = StatisticUnit.BYTES,
                      label = "Direct Memory Total Capacity",
                      description = "Total capacity of direct memory allocated for the Broker process")
    long getDirectMemoryTotalCapacity();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
                      units = StatisticUnit.COUNT,
                      label = "Number Of Object Pending Finalization",
                      description = "Number of objects pending finalization")
    int getNumberOfObjectsPendingFinalization();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
            units = StatisticUnit.COUNT,
            label = "Number of Buffers In-Use",
            description = "Number of direct memory buffers currently in-use.")
    long getNumberOfBuffersInUse();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
            units = StatisticUnit.COUNT,
            label = "Number of Pooled Buffers",
            description = "Number of unused direct memory buffers currently in the pool.")
    long getNumberOfBuffersInPool();

    @SuppressWarnings("unused")
    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
            units = StatisticUnit.BYTES,
            label = "Maximum recorded size of inbound messages",
            description = "Maximum size of messages published into the Broker since start-up.")
    long getInboundMessageSizeHighWatermark();

    @ManagedOperation(nonModifying = true,
            description = "Restart the broker within the same JVM",
            changesConfiguredObjectState = false,
            log = true)
    void restart();

    @ManagedOperation(nonModifying = true,
            description = "Initiates garbage collection",
            changesConfiguredObjectState = false)
    void performGC();

    @ManagedOperation(nonModifying = true,
                      description = "Collects thread stack traces and dead locks. Dumps stack traces into logs if requested",
            changesConfiguredObjectState = false)
    Content getThreadStackTraces(@Param(name="appendToLog",
                                        defaultValue = "false",
                                        description = "If true, appends stack traces into logs")
                                 boolean appendToLog);

    @ManagedOperation(nonModifying = true,
            description = "Collects thread stack traces for the threads with names containing matching characters for given regular expression",
            changesConfiguredObjectState = false)
    Content findThreadStackTraces(@Param(name="threadNameFindExpression",
                                        description = "Regular expression to find threads with names containing matching characters")
                                 String threadNameFindExpression);

    @ManagedOperation(nonModifying = true,
            description = "Returns the principal of the currently authenticated user",
            changesConfiguredObjectState = false,
            skipAclCheck = true)
    Principal getUser();

    @ManagedOperation(nonModifying = true,
            description = "Returns metadata concerning the current connection",
            changesConfiguredObjectState = false,
            skipAclCheck = true)
    SocketConnectionMetaData getConnectionMetaData();


    @ManagedOperation(nonModifying = true,
            description = "Returns the groups to which the currently authenticated user belongs",
            changesConfiguredObjectState = false,
            skipAclCheck = true)
    Set<Principal> getGroups();

    @ManagedOperation(description = "Removes a user and all associated preferences from the broker's configuration",
            changesConfiguredObjectState = true)
    void purgeUser(@Param(name="origin", description="The AuthenticationProvider the username is associated with")AuthenticationProvider<?> origin,
                   @Param(name="username", description="The unqualified username that should be purged from the broker", mandatory = true)String username);

    //children
    Collection<VirtualHostNode<?>> getVirtualHostNodes();

    Collection<Port<?>> getPorts();

    Collection<AuthenticationProvider<?>> getAuthenticationProviders();

    Collection<AccessControlProvider<?>> getAccessControlProviders();

    NamedAddressSpace getSystemAddressSpace(String name);

    Collection<GroupProvider<?>> getGroupProviders();

    VirtualHost<?> findVirtualHostByName(String name);

    VirtualHostNode findDefautVirtualHostNode();

    Collection<KeyStore<?>> getKeyStores();

    Collection<TrustStore<?>> getTrustStores();

    boolean isManagementMode();

    @Override
    EventLogger getEventLogger();

    @Override
    void setEventLogger(EventLogger eventLogger);

    boolean isVirtualHostPropertiesNodeEnabled();

    @Override
    AuthenticationProvider<?> getManagementModeAuthenticationProvider();

    void assignTargetSizes();

    @Override
    int getNetworkBufferSize();

    ScheduledFuture<?> scheduleHouseKeepingTask(long period, final TimeUnit unit, Runnable task);

    ScheduledFuture<?> scheduleTask(long delay, final TimeUnit unit, Runnable task);

    @DerivedAttribute(description = "Threshold direct memory size (in bytes) at which the Broker will start flowing incoming messages to disk.")
    long getFlowToDiskThreshold();

    @DerivedAttribute(description = "Threshold direct memory size (in bytes) at which the Broker will start considering to compact sparse buffers. Set to -1 to disable.")
    long getCompactMemoryThreshold();

    @DerivedAttribute(description = "Time interval (in milliseconds) between runs of the memory compactor check. See also " + COMPACT_MEMORY_THRESHOLD)
    long getCompactMemoryInterval();

    @DerivedAttribute(description = "Minimum fraction of direct memory buffer that can be occupied before the buffer is considered for compaction")
    double getSparsityFraction();

    @DerivedAttribute()
    long getHousekeepingCheckPeriod();

    @ManagedOperation(changesConfiguredObjectState = false, nonModifying = true,
            description = "Force direct memory buffer compaction.")
    void compactMemory();
}
