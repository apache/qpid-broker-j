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

import org.apache.qpid.configuration.CommonProperties;
import org.apache.qpid.server.logging.EventLogger;
import org.apache.qpid.server.logging.EventLoggerProvider;
import org.apache.qpid.server.stats.StatisticsGatherer;
import org.apache.qpid.server.store.preferences.UserPreferencesCreator;

@ManagedObject( defaultType = Broker.BROKER_TYPE)
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
    String STATISTICS_REPORTING_RESET_ENABLED = "statisticsReportingResetEnabled";
    String STORE_PATH = "storePath";
    String MODEL_VERSION = "modelVersion";
    String PREFERENCE_STORE_ATTRIBUTES   = "preferenceStoreAttributes";

    String CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT = "channel.flowControlEnforcementTimeout";

    String CONNECTION_SESSION_COUNT_LIMIT = "connection.sessionCountLimit";
    String CONNECTION_HEART_BEAT_DELAY = "connection.heartBeatDelay";
    String CONNECTION_CLOSE_WHEN_NO_ROUTE = "connection.closeWhenNoRoute";

    String BROKER_FLOW_TO_DISK_THRESHOLD = "broker.flowToDiskThreshold";
    String BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD = "broker.failStartupWithErroredChild";

    String BROKER_MSG_AUTH = "qpid.broker_msg_auth";

    String STORE_FILESYSTEM_MAX_USAGE_PERCENT = "store.filesystem.maxUsagePercent";
    String QPID_AMQP_PORT = "qpid.amqp_port";
    String QPID_HTTP_PORT = "qpid.http_port";
    String QPID_DOCUMENTATION_URL = "qpid.helpURL";

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
    long DEFAULT_FLOW_TO_DISK_THRESHOLD = (long)(0.4 * (double) BrokerImpl.getMaxDirectMemorySize());

    @ManagedContextDefault(name = CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT)
    long DEFAULT_CHANNEL_FLOW_CONTROL_ENFORCEMENT_TIMEOUT = 5000l;

    @ManagedContextDefault(name = STORE_FILESYSTEM_MAX_USAGE_PERCENT)
    int DEFAULT_FILESYSTEM_MAX_USAGE_PERCENT = 90;

    @ManagedContextDefault(name = BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD)
    boolean DEFAULT_BROKER_FAIL_STARTUP_WITH_ERRORED_CHILD = false;

    @ManagedContextDefault(name = BROKER_MSG_AUTH)
    boolean DEFAULT_BROKER_MSG_AUTH = false;

    @ManagedContextDefault(name = CommonProperties.QPID_SECURITY_TLS_PROTOCOL_WHITE_LIST)
    String DEFAULT_SECURITY_TLS_PROTOCOL_WHITE_LIST = "[\"TLSv1\\\\.[0-9]+\"]";

    @ManagedContextDefault(name = CommonProperties.QPID_SECURITY_TLS_PROTOCOL_BLACK_LIST)
    String DEFAULT_SECURITY_TLS_PROTOCOL_BLACK_LIST = "[\"TLSv1\\\\.0\"]";

    @ManagedContextDefault(name = CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_WHITE_LIST)
    String DEFAULT_SECURITY_TLS_CIPHER_SUITE_WHITE_LIST = "[]";

    @ManagedContextDefault(name = CommonProperties.QPID_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST)
    String DEFAULT_SECURITY_TLS_CIPHER_SUITE_BLACK_LIST = "[]";

    @ManagedContextDefault(name = QPID_DOCUMENTATION_URL)
    String DEFAULT_DOCUMENTATION_URL = "http://qpid.apache.org/releases/qpid-java-${qpid.version}/java-broker/book/";

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

    @ManagedAttribute( defaultValue = "256" )
    int getConnection_sessionCountLimit();

    @ManagedAttribute( defaultValue = "0")
    int getConnection_heartBeatDelay();

    @ManagedAttribute( defaultValue = "true" )
    boolean getConnection_closeWhenNoRoute();

    @ManagedAttribute( defaultValue = "0" )
    int getStatisticsReportingPeriod();

    @ManagedAttribute( defaultValue = "false")
    boolean getStatisticsReportingResetEnabled();


    @ManagedContextDefault( name = "broker.housekeepingThreadCount")
    public static final int DEFAULT_HOUSEKEEPING_THREAD_COUNT = 2;


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

    @ManagedAttribute(validValues = {"org.apache.qpid.server.model.BrokerImpl#getAvailableConfigurationEncrypters()"})
    String getConfidentialConfigurationEncryptionProvider();

    @DerivedAttribute( persist = true )
    String getModelVersion();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Inbound")
    long getBytesIn();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.BYTES, label = "Outbound")
    long getBytesOut();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Inbound")
    long getMessagesIn();

    @ManagedStatistic(statisticType = StatisticType.CUMULATIVE, units = StatisticUnit.MESSAGES, label = "Outbound")
    long getMessagesOut();

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

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
                      units = StatisticUnit.COUNT,
                      label = "Live threads",
                      description = "Number of live threads")
    int getNumberOfLiveThreads();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
                      units = StatisticUnit.BYTES,
                      label = "Used Heap Memory Size",
                      description = "Size of used heap memory")
    long getUsedHeapMemorySize();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
                      units = StatisticUnit.BYTES,
                      label = "Used Direct Memory Size",
                      description = "Size of used direct memory")
    long getUsedDirectMemorySize();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
                      units = StatisticUnit.BYTES,
                      label = "Direct Memory Total Capacity",
                      description = "Total capacity of direct memory allocated for the Broker process")
    long getDirectMemoryTotalCapacity();

    @ManagedStatistic(statisticType = StatisticType.POINT_IN_TIME,
                      units = StatisticUnit.COUNT,
                      label = "Number Of Object Pending Finalization",
                      description = "Number of objects pending finalization")
    int getNumberOfObjectsPendingFinalization();

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
            changesConfiguredObjectState = false)
    Principal getUser();

    @ManagedOperation(nonModifying = true,
            description = "Returns the groups to which the currently authenticated user belongs",
            changesConfiguredObjectState = false)
    Set<Principal> getGroups();

    @ManagedOperation(description = "Removes a user and all associated preferences from the broker's configuration",
            changesConfiguredObjectState = true)
    void purgeUser(@Param(name="origin", description="The AuthenticationProvider the username is associated with")AuthenticationProvider<?> origin,
                   @Param(name="username", description="The unqualified username that should be purged from the broker")String username);

    @ManagedOperation(description = "Resets statistics on this object and all child objects", changesConfiguredObjectState = false, nonModifying = true)
    void resetStatistics();

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

    EventLogger getEventLogger();

    void setEventLogger(EventLogger eventLogger);

    boolean isVirtualHostPropertiesNodeEnabled();

    AuthenticationProvider<?> getManagementModeAuthenticationProvider();

    void assignTargetSizes();

    int getNetworkBufferSize();

    ScheduledFuture<?> scheduleHouseKeepingTask(long period, final TimeUnit unit, Runnable task);

    ScheduledFuture<?> scheduleTask(long delay, final TimeUnit unit, Runnable task);

}
